import logging
import shutil
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import polars as pl
import requests
from d2d_library.db_queue import Queue
from d2d_library.dhis2_dataset_completion_handler import DatasetCompletionSync
from d2d_library.dhis2_extract_handlers import DHIS2Extractor
from d2d_library.dhis2_org_unit_aligner import DHIS2PyramidAligner
from d2d_library.dhis2_pusher import DHIS2Pusher
from dateutil.relativedelta import relativedelta
from openhexa.sdk import current_run, parameter, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dataframe import get_datasets
from openhexa.toolbox.dhis2.periods import period_from_string
from requests.exceptions import HTTPError, RequestException
from utils import (
    configure_logging_flush,
    connect_to_dhis2,
    load_configuration,
    read_parquet_extract,
    save_to_parquet,
    select_descendants,
)

# Ticket(s) related to this pipeline:
#   -https://bluesquare.atlassian.net/browse/SANRUSSC24-42
# github repo:
#   -https://github.com/BLSQ/openhexa-pipelines-sanru-ssc


@pipeline("iaso_dhis2_integration", timeout=43200)  # 3600 * 12 hours
@parameter(
    code="run_extract_data",
    name="Extract data",
    type=bool,
    default=True,
    help="Extract data elements from source DHIS2.",
)
@parameter(
    code="run_push_data",
    name="Push data",
    type=bool,
    default=True,
    help="Push data to target DHIS2.",
)
def iaso_dhis2_integration(run_extract_data: bool, run_push_data: bool):
    """Main pipeline function for DHIS2 dataset synchronization.

    Parameters
    ----------
    run_extract_data : bool, optional
        If True, runs the data extraction task (default is True).
    run_push_data : bool, optional
        If True, runs the data push task (default is True).

    Raises
    ------
    Exception
        If an error occurs during the pipeline execution.
    """
    pipeline_path = Path(workspace.files_path) / "pipelines" / "dhis2_dataset_sync"

    try:
        extract_data(
            pipeline_path=pipeline_path,
            run_task=run_extract_data,
            # wait=datasets_ready,
        )

        push_ready = push_data(
            pipeline_path=pipeline_path,
            run_task=run_push_data,
            # wait=datasets_ready,
        )

    except Exception as e:
        current_run.log_error(f"An error occurred: {e}")
        raise


@iaso_dhis2_integration.task
def extract_data(
    pipeline_path: Path,
    run_task: bool = True,
    wait: bool = True,
):
    """Extracts data elements from the source DHIS2 instance and saves them in parquet format."""
    if not run_task:
        current_run.log_info("Data elements extraction task skipped.")
        return

    current_run.log_info("Data elements extraction task started.")

    # load configuration
    extract_config = load_configuration(
        config_path=pipeline_path / "configuration" / "extract_config.json"
    )

    # connect to source DHIS2 instance (No cache for data extraction)
    dhis2_client = connect_to_dhis2(
        connection_str=extract_config["SETTINGS"]["SOURCE_DHIS2_CONNECTION"],
        cache_dir=None,
    )

    # NOTE: We need the filtered source pyramid to validate the org units (aligned org units).
    source_pyramid = read_parquet_extract(
        pipeline_path / "data" / "pyramid" / "pyramid_data.parquet"
    )

    # initialize queue
    db_path = pipeline_path / "configuration" / ".queue.db"
    push_queue = Queue(db_path)

    try:
        months_lag = extract_config["SETTINGS"].get(
            "NUMBER_MONTHS_WINDOW", 3
        )  # default 3 months window
        if not extract_config["SETTINGS"]["STARTDATE"]:
            start = (datetime.now() - relativedelta(months=months_lag)).strftime("%Y%m")
        else:
            start = extract_config["SETTINGS"]["STARTDATE"]
        if not extract_config["SETTINGS"]["ENDDATE"]:
            end = (datetime.now() - relativedelta(months=1)).strftime(
                "%Y%m"
            )  # go back 1 month.
        else:
            end = extract_config["SETTINGS"]["ENDDATE"]
    except Exception as e:
        raise Exception(f"Error in start/end date configuration: {e}") from e

    # limits
    dhis2_client.data_value_sets.MAX_DATA_ELEMENTS = 100
    dhis2_client.data_value_sets.MAX_ORG_UNITS = 100

    try:
        # Get periods
        start_period = period_from_string(start)
        end_period = period_from_string(end)
        extract_periods = (
            [str(p) for p in start_period.get_range(end_period)]
            if str(start_period) < str(end_period)
            else [str(start_period)]
        )
    except Exception as e:
        raise Exception(f"Error in start/end date configuration: {e!s}") from e

    download_settings = extract_config["SETTINGS"].get("MODE", None)
    if download_settings is None:
        download_settings = "DOWNLOAD_REPLACE"
        current_run.log_warning(
            f"No 'MODE' found in extraction settings. Set default: {download_settings}"
        )

    # Setup extractor
    # See docs about return_existing_file impact.
    dhis2_extractor = DHIS2Extractor(
        dhis2_client=dhis2_client,
        download_mode=download_settings,
        return_existing_file=False,
    )

    current_run.log_info(
        f"Download MODE: {extract_config['SETTINGS']['MODE']} from: {start} to {end}"
    )
    handle_data_element_extracts(
        pipeline_path=pipeline_path,
        dhis2_extractor=dhis2_extractor,
        data_element_extracts=extract_config["DATA_ELEMENTS"].get("EXTRACTS", []),
        source_pyramid=source_pyramid,
        extract_periods=extract_periods,
        push_queue=push_queue,
    )


@iaso_dhis2_integration.task
def push_data(
    pipeline_path: Path,
    run_task: bool = True,
    wait: bool = True,
) -> bool:
    """Pushes data elements to the target DHIS2 instance.

    Returns
    -------
    bool
        True: This is just a dummy flag to indicate the data push task is done.
    """
    if not run_task:
        current_run.log_info("Data push task skipped.")
        return True

    current_run.log_info("Starting data push.")

    # setup
    logger, logs_file = configure_logging_flush(
        logs_path=Path("/home/jovyan/tmp/logs"), task_name="push_data"
    )
    config = load_configuration(
        config_path=pipeline_path / "configuration" / "push_config.json"
    )
    target_dhis2 = connect_to_dhis2(
        connection_str=config["SETTINGS"]["TARGET_DHIS2_CONNECTION"], cache_dir=None
    )
    db_path = pipeline_path / "configuration" / ".queue.db"
    push_queue = Queue(db_path)

    # Push parameters
    import_strategy = config["SETTINGS"].get("IMPORT_STRATEGY", "CREATE_AND_UPDATE")
    dry_run = config["SETTINGS"].get("DRY_RUN", True)
    max_post = config["SETTINGS"].get("MAX_POST", 500)
    push_wait = config["SETTINGS"].get("PUSH_WAIT_MINUTES", 5)

    # log parameters
    logger.info(
        f"Import strategy: {import_strategy} - Dry Run: {dry_run} - Max Post elements: {max_post}"
    )
    current_run.log_info(
        f"Pushing data with parameters import_strategy: {import_strategy}, dry_run: {dry_run}, max_post: {max_post}"
    )

    # Set up DHIS2 pusher
    pusher = DHIS2Pusher(
        dhis2_client=target_dhis2,
        import_strategy=import_strategy,
        dry_run=dry_run,
        max_post=max_post,
        logger=logger,
    )

    # Map data types to their respective mapping functions
    dispatch_map = {
        "DATA_ELEMENT": (
            config["DATA_ELEMENTS"]["EXTRACTS"],
            apply_data_element_extract_config,
        ),
    }

    # loop over the queue
    while True:
        next_extract = push_queue.peek()
        if next_extract == "FINISH":
            push_queue.dequeue()  # remove marker if present
            break

        if not next_extract:
            current_run.log_info("Push data process: waiting for updates")
            time.sleep(60 * int(push_wait))
            continue

        try:
            # Read extract
            extract_id, extract_file_path = split_on_pipe(next_extract)
            extract_path = Path(extract_file_path)
            extract_data = read_parquet_extract(parquet_file=extract_path)
        except Exception as e:
            current_run.log_error(
                f"Failed to read extract from queue item: {next_extract}. Error: {e}"
            )
            push_queue.dequeue()  # remove problematic item
            continue

        try:
            # Determine data type
            data_type = extract_data["DATA_TYPE"].unique()[0]
            period = extract_data["PERIOD"].unique()[0]

            current_run.log_info(
                f"Pushing data for extract {extract_id}: {extract_path.name}."
            )
            if data_type not in dispatch_map:
                current_run.log_warning(
                    f"Unknown DATA_TYPE '{data_type}' in extract: {extract_path.name}. Skipping."
                )
                push_queue.dequeue()  # remove unknown item
                continue

            # Get config and mapping function
            cfg_list, mapper_func = dispatch_map[data_type]
            extract_config = next(
                (e for e in cfg_list if e["EXTRACT_UID"] == extract_id), {}
            )

            # Apply mapping and push data
            df_mapped: pd.DataFrame = mapper_func(
                df=extract_data, extract_config=extract_config
            )
            # df_mapped[[""]].drop_duplicates().head()
            df_mapped = df_mapped.sort_values(
                by="ORG_UNIT"
            )  # speed up DHIS2 processing
            pusher.push_data(df_data=df_mapped)

            # Success → dequeue
            push_queue.dequeue()
            current_run.log_info(
                f"Data push finished for extract: {extract_path.name}."
            )

        except Exception as e:
            current_run.log_error(
                f"Fatal error for extract {extract_id} ({extract_path.name}), stopping push process. Error: {e!s}"
            )
            raise  # crash on error

        finally:
            save_logs(logs_file, output_dir=pipeline_path / "logs" / "push")
    return True


def save_logs(logs_file: Path, output_dir: Path) -> None:
    """Moves all .log files from logs_path to output_dir."""
    output_dir.mkdir(parents=True, exist_ok=True)
    if logs_file.is_file():
        dest_file = output_dir / logs_file.name
        shutil.copy(logs_file.as_posix(), dest_file.as_posix())


def handle_data_element_extracts(
    pipeline_path: Path,
    dhis2_extractor: DHIS2Extractor,
    data_element_extracts: list,
    source_pyramid: pd.DataFrame,
    extract_periods: list[str],
    push_queue: Queue,
):
    """Handles data elements extracts based on the configuration."""
    if len(data_element_extracts) == 0:
        current_run.log_info("No data elements to extract.")
        return

    logger, logs_file = configure_logging_flush(
        logs_path=Path("/home/jovyan/tmp/logs"), task_name="extract_data"
    )
    current_run.log_info("Starting data element extracts.")
    try:
        # loop over the available extract configurations
        for idx, extract in enumerate(data_element_extracts):
            extract_id = extract.get("EXTRACT_UID")
            org_units_level = extract.get("ORG_UNITS_LEVEL", None)
            data_element_uids = extract.get("UIDS", [])

            if extract_id is None:
                current_run.log_warning(
                    f"No 'EXTRACT_UID' defined for extract position: {idx}. This is required, extract skipped."
                )
                continue

            if org_units_level is None:
                current_run.log_warning(
                    f"No 'ORG_UNITS_LEVEL' defined for extract: {extract_id}, extract skipped."
                )
                continue

            if len(data_element_uids) == 0:
                current_run.log_warning(
                    f"No data elements defined for extract: {extract_id}, extract skipped."
                )
                continue

            # get org units from the filtered pyramid
            org_units = source_pyramid[source_pyramid["level"] == org_units_level][
                "id"
            ].to_list()
            current_run.log_info(
                f"Starting data elements extract ID: '{extract_id}' ({idx + 1}) "
                f"with {len(data_element_uids)} data elements across {len(org_units)} org units "
                f"(level {org_units_level})."
            )

            # run data elements extraction per period
            for period in extract_periods:
                try:
                    extract_path = dhis2_extractor.data_elements.download_period(
                        data_elements=data_element_uids,
                        org_units=org_units,
                        period=period,
                        output_dir=pipeline_path
                        / "data"
                        / "extracts"
                        / "data_elements"
                        / f"extract_{extract_id}",
                    )
                    if extract_path is not None:
                        push_queue.enqueue(f"{extract_id}|{extract_path}")

                except Exception as e:
                    current_run.log_warning(
                        f"Extract {extract_id} download failed for period {period}, skipping to next extract."
                    )
                    logger.error(f"Extract {extract_id} - period {period} error: {e}")
                    break  # skip to next extract

            current_run.log_info(f"Extract {extract_id} finished.")

    finally:
        push_queue.enqueue("FINISH")
        save_logs(logs_file, output_dir=pipeline_path / "logs" / "extract")


def apply_data_element_extract_config(
    df: pd.DataFrame, extract_config: dict, logger: logging.Logger | None = None
) -> pd.DataFrame:
    """Applies data element mappings to the extracted data.

    It also filters data elements based on category option combo (COC) if specified in the extract configuration.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing the extracted data.
    extract_config : dict
        This is a dictionary containing the extract mappings.
    logger : logging.Logger, optional
        Logger instance for logging (default is None).

    Returns
    -------
    pd.DataFrame
        DataFrame with mapped data elements.
    """
    if len(extract_config) == 0:
        current_run.log_warning(
            "No extract details provided, skipping data element mappings."
        )
        return df

    extract_mappings = extract_config.get("MAPPINGS", {})
    if len(extract_mappings) == 0:
        current_run.log_warning(
            "No extract mappings provided, skipping data element mappings."
        )
        return df

    # Loop over the configured data element mappings to filter by COC/AOC if provided
    current_run.log_info(
        f"Applying data element mappings for extract: {extract_config.get('EXTRACT_UID')}."
    )
    chunks = []
    uid_mappings = {}
    for uid, mapping in extract_mappings.items():
        uid_mapping = mapping.get("UID")
        coc_mappings = mapping.get("CATEGORY_OPTION_COMBO", {})
        aoc_mappings = mapping.get("ATTRIBUTE_OPTION_COMBO", {})

        # Build a mask selection
        df_uid = df[df["DX_UID"] == uid].copy()
        if coc_mappings:
            coc_mappings = {
                k: v for k, v in coc_mappings.items() if v is not None
            }  # Do not replace with None
            coc_mappings_clean = {
                str(k).strip(): str(v).strip() for k, v in coc_mappings.items()
            }
            df_uid = df_uid[
                df_uid["CATEGORY_OPTION_COMBO"].isin(coc_mappings_clean.keys())
            ]
            df_uid["CATEGORY_OPTION_COMBO"] = df_uid.loc[
                :, "CATEGORY_OPTION_COMBO"
            ].replace(coc_mappings_clean)

        if aoc_mappings:
            aoc_mappings = {
                k: v for k, v in aoc_mappings.items() if v is not None
            }  # Do not replace with None
            aoc_mappings_clean = {
                str(k).strip(): str(v).strip() for k, v in aoc_mappings.items()
            }
            df_uid = df_uid[
                df_uid["ATTRIBUTE_OPTION_COMBO"].isin(aoc_mappings_clean.keys())
            ]
            df_uid["ATTRIBUTE_OPTION_COMBO"] = df_uid.loc[
                :, "ATTRIBUTE_OPTION_COMBO"
            ].replace(aoc_mappings_clean)

        if uid_mapping:
            uid_mappings[uid] = uid_mapping

        chunks.append(df_uid)

    if len(chunks) == 0:
        current_run.log_warning(
            "No data elements matched the provided mappings, returning empty dataframe."
        )
        logger.warning(
            "No data elements matched the provided mappings, returning empty dataframe."
        )
        return pd.DataFrame(columns=df.columns)

    df_filtered = pd.concat(chunks, ignore_index=True)

    if uid_mappings:
        uid_mappings = {
            k: v for k, v in uid_mappings.items() if v is not None
        }  # Do not replace with None
        uid_mappings_clean = {
            str(k).strip(): str(v).strip() for k, v in uid_mappings.items()
        }
        df_filtered["DX_UID"] = df_filtered.loc[:, "DX_UID"].replace(uid_mappings_clean)

    return df_filtered


def split_on_pipe(s: str) -> tuple[str, str | None]:
    """Splits a string on the first pipe character and returns a tuple.

    Parameters
    ----------
    s : str
        The string to split.

    Returns
    -------
    tuple[str, str | None]
        A tuple containing the part before the pipe and the part after the pipe (or None if no pipe is found).
    """
    parts = s.split("|", 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return None, parts[0]


if __name__ == "__main__":
    iaso_dhis2_integration()
