import logging
import shutil
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import polars as pl
from d2d_library.db_queue import Queue
from d2d_library.dhis2_pusher import DHIS2Pusher
from dateutil.relativedelta import relativedelta
from openhexa.sdk import current_run, parameter, pipeline, workspace
from utils import (
    configure_logging_flush,
    connect_to_dhis2,
    load_configuration,
    read_parquet_extract,
    get_periods,
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
        )

        push_data(
            pipeline_path=pipeline_path,
            run_task=run_push_data,
        )

    except Exception as e:
        current_run.log_error(f"An error occurred: {e}")
        raise


@iaso_dhis2_integration.task
def extract_data(
    pipeline_path: Path,
    run_task: bool = True,
):
    """Extracts data elements from the source DHIS2 instance and saves them in parquet format."""
    if not run_task:
        current_run.log_info("Data extraction task skipped.")
        return
    current_run.log_info("Data extraction task started.")

    extract_config = load_configuration(config_path=pipeline_path / "configuration" / "extract_config.json")
    iaso_client = connect_to_iaso_or_something(extract_config["SETTINGS"]["IASO_CONNECTION"])  # iaso CLIENT

    # initialize queue (always pointing to the same db file)
    db_path = pipeline_path / "configuration" / ".queue.db"
    push_queue = Queue(db_path)

    handle_iaso_extracts(
        pipeline_path=pipeline_path,
        iaso_client=iaso_client,
        extract_configurations=extract_config["DATA_ELEMENTS"].get("EXTRACTS", []),
        push_queue=push_queue,
    )


@iaso_dhis2_integration.task
def push_data(
    pipeline_path: Path,
    run_task: bool = True,
) -> None:
    """Pushes data elements to the target DHIS2 instance."""
    if not run_task:
        current_run.log_info("Data push task skipped.")
        return True

    current_run.log_info("Starting data push.")

    # setup
    logger, logs_file = configure_logging_flush(
        logs_path=Path("/home/jovyan/tmp/logs"), task_name="push_data"
    )
    config = load_configuration(config_path=pipeline_path / "configuration" / "push_config.json")
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
    logger.info(f"Import strategy: {import_strategy} - Dry Run: {dry_run} - Max Post elements: {max_post}")
    current_run.log_info(
        f"Pushing data with parameters import_strategy: {import_strategy}, "
        f"dry_run: {dry_run}, max_post: {max_post}"
    )

    # Set up DHIS2 pusher
    pusher = DHIS2Pusher(
        dhis2_client=target_dhis2,
        import_strategy=import_strategy,
        dry_run=dry_run,
        max_post=max_post,
        logger=logger,
    )

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
            current_run.log_error(f"Failed to read extract from queue item: {next_extract}. Error: {e}")
            push_queue.dequeue()  # remove problematic item
            continue

        try:
            current_run.log_info(f"Pushing data for extract {extract_id}: {extract_path.name}.")

            # Get corresponding config for extract and apply mapping and push data
            extract_config = next(
                (e for e in config["CMM_INDICATORS"]["EXTRACTS"] if e["EXTRACT_UID"] == extract_id),
                {},
            )
            df_mapped: pd.DataFrame = apply_data_element_extract_config(
                df=extract_data, extract_config=extract_config
            )

            # PUSH
            pusher.push_data(df_data=df_mapped)

            # Success → dequeue
            push_queue.dequeue()
            current_run.log_info(f"Data push finished for extract: {extract_path.name}.")

        except Exception as e:
            current_run.log_error(
                f"Fatal error for extract {extract_id} ({extract_path.name}),"
                f" stopping push process. Error: {e!s}"
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


def handle_iaso_extracts(
    pipeline_path: Path,
    # iaso_client: IASO client or something,
    extract_configurations: list,
    push_queue: Queue,
):
    """Handles data elements extracts based on the configuration."""
    logger, logs_file = configure_logging_flush(
        logs_path=Path("/home/jovyan/tmp/logs"), task_name="extract_data"
    )
    current_run.log_info("Starting data extracts.")
    try:
        # loop over the available extract configurations
        for idx, extract in enumerate(extract_configurations):
            extract_id = extract.get("EXTRACT_UID")
            data_element_uids = extract.get("UIDS", [])  # not sure if relevant in our case, remove otherwise.

            if extract_id is None:
                current_run.log_warning(
                    f"No 'EXTRACT_UID' defined for extract position: {idx}. "
                    f"This is required, extract skipped."
                )
                continue

            if len(data_element_uids) == 0:
                current_run.log_warning(
                    f"No data elements defined for extract: {extract_id}, extract skipped."
                )
                continue

            extract_mode = extract.get("MODE", "DOWNLOAD_REPLACE")

            # resolve periods
            start, end = resolve_extraction_window(extract)
            extract_periods = get_periods(start, end)
            current_run.log_info(f"Downloading with mode: {extract_mode} from: {start} to {end}")

            # run data elements extraction per period
            for period in extract_periods:
                try:
                    ## (!) HERE we can implement a DOWNLOAD MODE:
                    ## IF mode == "DOWNLOAD_REPLACE": we download the extract and replace an existing file (same period).
                    ## ELSE mode == "DOWNLOAD_NEW": we download only those extracts that do not yet exist.

                    #   To ensure that only new (relevant) data is sent to DHIS2, the extraction pipeline can:
                    #   1. Download all IASO data for each period.
                    #   2. If a file for that period already exists, compare each row using composite key (orgUnit,dataElementID,period)
                    #     .. Would be nice to have this key parameterized so it can be used in other pipelines
                    #   3. Mark each data point (row):
                    #       - SET Status: "new" if the datapoint do not exist in the extract.
                    #       - SET Status: "new" if the datapoint "VALUE" has changed.
                    #
                    #   4. Push task will select only datapoints with status "new" to be pushed to DHIS2.
                    #   5. Push task sets datapoints (rows) as "imported", if it was correctly pushed to DHIS2

                    # EXAMPLE CODE:
                    ## HERE WE SHOULD BE GETTING THE PATH TO THE EXTRACTED FILE FROM IASO
                    ## THEN WE PUSH THIS PATH TO THE QUEUE TO BE PICKED UP BY THE PUSH TASK
                    # extract_path = retrieve_extract(
                    #     iaso_client=iaso_client,
                    #     period=period,
                    #     output_dir=pipeline_path / "data"/ "extracts"/ "data_elements"/ f"{extract_id}",
                    # )

                    # if extract_path is not None:
                    # push_queue.enqueue(
                    # f"{extract_id}|{extract_path}"
                    # )  ## I SAVE THE EXTRACT UID WITH THE PATH 'ID|extract_path'

                    pass  # remove this when the actual extraction code is uncommented
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


def resolve_extraction_window(settings: dict) -> tuple[str, str]:
    """Returns (start_yyyymm, end_yyyymm) based on settings dict.

    Returns
    -------
    tuple[str, str]
        A tuple containing the start and end dates in 'YYYYMM' format.
    """
    months_lag = settings.get("NUMBER_MONTHS_WINDOW", 3)

    if "START_PERIOD" in settings:
        start = settings["START_PERIOD"]
        _validate_yyyymm(start, "START_PERIOD")
    else:
        start = (datetime.now() - relativedelta(months=months_lag)).strftime("%Y%m")

    if "END_PERIOD" in settings:
        end = settings["END_PERIOD"]
        _validate_yyyymm(end, "END_PERIOD")
    else:
        end = (datetime.now() - relativedelta(months=1)).strftime("%Y%m")

    return start, end


def _validate_yyyymm(value: str, field: str):
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string in YYYYMM format")

    try:
        datetime.strptime(value, "%Y%m")
    except ValueError as e:
        raise ValueError(f"{field} must be in YYYYMM format, got {value}") from e


def apply_data_element_extract_config(
    df: pd.DataFrame, extract_config: dict, logger: logging.Logger | None = None
) -> pd.DataFrame:
    """Applies data element mappings to the CMM indicators.

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
        current_run.log_warning("No extract details provided, skipping data element mappings.")
        return df

    extract_mappings = extract_config.get("MAPPINGS", {})
    if len(extract_mappings) == 0:
        current_run.log_warning("No extract mappings provided, skipping data element mappings.")
        return df

    # Loop over the configured data element mappings to filter by COC/AOC if provided
    current_run.log_info(f"Applying data element mappings for extract: {extract_config.get('EXTRACT_UID')}.")
    chunks = []
    for uid, mapping in extract_mappings.items():
        uid_mapping = mapping.get("DX_UID")
        coc_mapping = mapping.get("CATEGORY_OPTION_COMBO")
        aoc_mapping = mapping.get("ATTRIBUTE_OPTION_COMBO")

        # select indicator data
        df_indicator = df[df["INDICATOR"] == uid].copy()
        if coc_mapping:
            df_indicator["CATEGORY_OPTION_COMBO"] = coc_mapping.strip()

        if aoc_mapping:
            df_indicator["ATTRIBUTE_OPTION_COMBO"] = aoc_mapping.strip()

        if uid_mapping:
            df_indicator["DX_UID"] = uid_mapping.strip()

        chunks.append(df_indicator)

    if len(chunks) == 0:
        current_run.log_warning("No data elements matched the provided mappings, returning empty dataframe.")
        logger.warning("No data elements matched the provided mappings, returning empty dataframe.")
        return pd.DataFrame(columns=df.columns)

    df_mapped = pd.concat(chunks, ignore_index=True)
    df_mapped["VALUE"] = (
        df_mapped["VALUE"]
        .where(df_mapped["VALUE"].abs() >= 1e-9, 0)  # kill float noise
        .round(4)  # round to 4 decimal places
    )
    return df_mapped.sort_values(by="ORG_UNIT")


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
