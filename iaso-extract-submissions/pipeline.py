"""Pipeline for extracting and processing form submissions from IASO."""

from __future__ import annotations


import re
import unicodedata
from datetime import datetime
from io import StringIO
from pathlib import Path
import json
from typing import Any

import polars as pl
from openhexa.sdk import (
    DHIS2Connection,
    IASOConnection,
    current_run,
    parameter,
    pipeline,
    workspace,
)

from openhexa.toolbox.iaso import IASO, dataframe
from openhexa.toolbox.dhis2 import DHIS2
from d2d_library.dhis2_pusher import DHIS2Pusher

# Precompile regex pattern for string cleaning
CLEAN_PATTERN = re.compile(r"[^\w\s-]")


@pipeline("sanru_iaso_to_dhis2", timeout=43200)
@parameter(
    "last_updated",
    name="Last Updated Date",
    type=str,
    required=False,
    help="ISO formatted date (YYYY-MM-DD) for incremental data extraction",
)
@parameter(
    "dry_run",
    name="Dry Run importation to DHIS2",
    type=bool,
    required=False,
    default=False,
    help="Test run while making sure data is not written on target.",
)
@parameter(
    "extract",
    name="Extract",
    type=bool,
    required=True,
    default=True,
    help="Run extraction of data from IASO",
)
@parameter(
    "transform",
    name="Transform",
    type=bool,
    required=True,
    default=True,
    help="Run transformation of data already extraacted from IASO",
)
@parameter(
    "post_to_dhis2",
    name="Post to DHIS2",
    type=bool,
    required=True,
    default=True,
    help="Run pushing of transformed data to DHIS2",
)
def sanru_iaso_to_dhis2(
    last_updated: str | None,
    dry_run: bool,
    extract: bool,
    transform: bool,
    post_to_dhis2: bool
):
    """Pipeline orchestration function for extracting and processing form submissions."""
    current_run.log_info("Starting form submissions extraction pipeline")

    # read default cofigurations tailored to this pipeline
    with Path(
        workspace.files_path,
        "pipelines/sanru-iaso-to-dhis2/configurations/default_values.json",
    ).open(encoding="utf-8") as file:
        configurations = json.loads(file.read())
    form_id = configurations["form_id"]
    choices_to_labels = configurations["choices_to_labels"]
    output_file_name = configurations["output_file_name"]
    output_format = configurations["output_format"]

    dhis2_connection = workspace.dhis2_connection(configurations["dhis2_connection"])
    iaso_connection = workspace.iaso_connection(configurations["iaso_connection"])

    form_name = get_form_name(iaso_connection, form_id)
    output_file_path = extract_and_load(
        form_id,
        last_updated,
        choices_to_labels,
        output_file_name,
        output_format,
        form_name,
        iaso_connection,
        extract
    )
    place_holder = process_and_transform(
        form_name, output_format, output_file_name, output_file_path, transform
    )

    post_to_dhis2_task(dhis2_connection, dry_run, output_format, place_holder, post_to_dhis2)


@sanru_iaso_to_dhis2.task
def extract_and_load(  # noqa: ANN201
    form_id: str,
    last_updated: str,
    choices_to_labels: str,
    output_file_name: str,
    output_format: str,
    form_name: str,
    iaso_connection: IASO,
    extract: bool
):
    """Task to extract and save data to the workspace.

    Returns:
    ________
        output_file_path str: Path to where data is saved
    """
    if not extract:
        current_run.log_info("Skipping data extraction task.")
        return None

    try:

        cutoff_date = parse_cutoff_date(last_updated)
        iaso = authenticate_iaso(iaso_connection)
        submissions = fetch_submissions(iaso, form_id, cutoff_date)
        submissions = process_choices(submissions, choices_to_labels, iaso, form_id)
        submissions = deduplicate_columns(submissions)
        output_file_path = export_to_file(
            submissions, form_name, output_file_name, output_format, True
        )
        current_run.log_info(f"Data exported to file: `{output_file_path}`")
        current_run.log_info("Data extraction successful ✅")

        return output_file_path

    except Exception as exc:
        current_run.log_error(f"Pipeline failed: {exc}")
        raise


@sanru_iaso_to_dhis2.task
def process_and_transform(
    form_name: str, output_format: str, output_file_name: str, output_file_path: str, transform: str
):
    """Task to process the data saved in the workspace."""
    if not transform:
        current_run.log_info("Skipping data transform task.")
        return

    current_run.log_info("Processing and transforming data.")
    try:
        # process the extracted data
        transform_data(output_format, form_name, output_file_name)
        current_run.log_info("Transformation successful.")
    except Exception as exc:
        current_run.log_error(f"Pipeline failed: {exc}")
        raise


@sanru_iaso_to_dhis2.task
def post_to_dhis2_task(
    dhis2_connection: DHIS2Connection, dry_run: bool, output_format: str, place_holder: int, post_to_dhis2: bool
):
    """Task to post data to dhis2."""
    if not post_to_dhis2:
        current_run.log_info("Skipping posting data to taarget DHIS2 instance.")
        return

    current_run.log_info("Posting data to dhis2.")
    _post_handler(dhis2_connection, dry_run, output_format)


def _post_handler(dhis2_connection: DHIS2Connection, dry_run: bool, output_format: str):
    # initialize_target dhis2_connectiont dhis2 instance
    target_dhis2_instance = validate_connections(dhis2_connection)
    pusher = DHIS2Pusher(
        dhis2_client=target_dhis2_instance,
        import_strategy="CREATE_AND_UPDATE",
        dry_run=dry_run,
        max_post=1000,
    )
    folder_path = Path(workspace.files_path, "pipelines/sanru-iaso-to-dhis2/transformed")
    for transformed_file in list(folder_path.glob(f"*{output_format}")):
        if output_format == ".csv":
            transformed_data = pl.read_csv(
                transformed_file, infer_schema_length=1000, ignore_errors=True
            )
        elif output_format == ".parquet":
            transformed_data = pl.read_parquet(transformed_file)

        post_to_dhis2(pusher, transformed_data, dry_run)


def get_dhis2_client(connection: DHIS2Connection) -> DHIS2:
    """Initialize DHIS2 client with caching.

    Returns:
        DHIS2: Configured DHIS2 client instance.
    """
    cache_dir = Path(workspace.files_path) / ".cache"
    return DHIS2(connection=connection, cache_dir=cache_dir)


def validate_connections(
    dhis2_connection: DHIS2Connection,
) -> tuple[DHIS2, DHIS2]:
    """Validate source and target DHIS2 connections.

    Returns:
        tuple[DHIS2, DHIS2]: Source and target DHIS2 client instances.
    """
    current_run.log_info("Validating DHIS2 connections...")

    # Initialize clients
    dhis2 = get_dhis2_client(dhis2_connection)

    # Test source connection
    try:
        dhis2.ping()
        msg = f"✓ Source DHIS2 connection successful: {dhis2.api.url}"
        current_run.log_info(msg)
    except Exception as e:
        current_run.log_error(f"✗ Source DHIS2 connection failed: {e!s}")
        raise

    return dhis2


def yes_no_to_int(colname: str, alias: str) -> pl.Expr:
    """Convert yes/no or 1/0 values to an integer indicator column.

    Args:
        colname (str): Name of the source column to convert. The column
            will be cast to string before evaluation.
        alias (str): Name of the output column.

    Returns:
        pl.Expr: A Polars expression producing an integer column where
            "yes" or "1" maps to 1, "no" or "0" maps to 0, and all other
            values (including nulls) result in null.

    Raises:
        ValueError: If the column does not exist in the DataFrame
            (raised by Polars during query execution).
    """
    col = pl.col(colname).cast(pl.Utf8)

    return (
        pl.when(col.is_in(["1", "yes"]))
        .then(1)
        .when(col.is_in(["0", "no"]))
        .then(0)
        .otherwise(None)
        .alias(alias)
    )


def transform_data(  # noqa: D103
    output_format: str, form_name: str, output_file_name: str, process_all: bool = True
) -> None:
    # read data from raw

    with Path(
        workspace.files_path,
        "pipelines/sanru-iaso-to-dhis2/configurations/mappings.json",
    ).open(encoding="utf-8") as file:
        mapping_df = pl.DataFrame(json.loads(file.read()))

    folder_path = Path(workspace.files_path, "pipelines/sanru-iaso-to-dhis2/raw")

    for raw_file in list(folder_path.glob(f"*{output_format}")):
        if output_format == ".csv":
            supervision = pl.read_csv(
                raw_file, infer_schema_length=1000, ignore_errors=True
            )
        elif output_format == ".parquet":
            supervision = pl.read_parquet(raw_file)

        supervision = _process_columns(supervision)

        # TBD: only process the updated files. if process_all is True
        # TBD: process only files from a certain region/ province/ ou

        supervision = supervision.unpivot(
            index=["export_id", "periode", "reference_externe"],
            variable_name="data_element",
            value_name="value",
        )
        supervision = supervision.with_columns(
            pl.col("data_element").str.to_lowercase()
        )
        mapping_df = mapping_df.with_columns(pl.col("name").str.to_lowercase())

        transformed_data = supervision.join(
            mapping_df, left_on="data_element", right_on="name", how="left"
        )

        transformed_data = transformed_data.drop(["Calcul", "data_element", "CC", "export_id"])
        transformed_data = transformed_data.rename({
            "periode": "PERIOD",
            "reference_externe": "ORG_UNIT",
            "data_element_id": "DX_UID",
            "value": "VALUE",
            "COC": "CATEGORY_OPTION_COMBO"
        })

        transformed_data = transformed_data.with_columns(
            pl.lit("DATA_ELEMENT").alias("DATA_TYPE")
            )
        
        transformed_data = transformed_data.with_columns(
            pl.when(
                pl.col("CATEGORY_OPTION_COMBO").is_null() |
                (pl.col("CATEGORY_OPTION_COMBO") == "")  # noqa: PLC1901
                )
            .then(pl.lit("HllvX50cXC0"))
            .otherwise(pl.col("CATEGORY_OPTION_COMBO"))
            .alias("CATEGORY_OPTION_COMBO")
            )

        transformed_data = transformed_data.with_columns(
            pl.lit("HllvX50cXC0").alias("ATTRIBUTE_OPTION_COMBO")
            )

        current_period = int(datetime.now().strftime("%Y%m"))
        transformed_data = transformed_data.filter(pl.col("PERIOD") <= current_period)
        transformed_data = transformed_data.filter(
            pl.col("ORG_UNIT").is_not_null()
            & (pl.col("ORG_UNIT").str.strip_chars() != "")  # noqa: PLC1901
        )
        # remove unmapped org units
        transformed_data = transformed_data.filter(
            ~pl.col("ORG_UNIT").str.starts_with("iaso#")
        )
        transformed_data = transformed_data.filter(
            ~pl.col("ORG_UNIT").str.starts_with("ssc")
        )
        if transformed_data.is_empty():
            continue

        export_to_file(
            transformed_data.drop_nulls(),
            form_name,
            output_file_name,
            output_format,
            False,
            transformed_file_path=str(raw_file).replace("raw", "transformed"),
        )


def _process_columns(data: pl.DataFrame) -> pl.DataFrame:
    num = pl.Float64
    data = data.with_columns(
        [
            (
                pl.col("amoxy_qc_moins_de_1an").cast(num)
                + pl.col("amoxy_qc_1an_a_5ans").cast(num)
            ).alias("amoxy_qc_moins_de_1an_plus_amoxy_qc_1an_a_5ans"),
            (pl.col("sro_qc").cast(num) / 2).alias("sro_qc_divide_2"),
            (pl.col("sro_si").cast(num) / 2).alias("sro_si_divide_2"),
            (pl.col("sro_sd").cast(num) / 2).alias("sro_sd_divide_2"),
            (pl.col("sro_in").cast(num) / 2).alias("sro_in_divide_2"),
            (pl.col("sro_jrs").cast(num) / 2).alias("sro_jrs_divide_2"),
            (
                pl.col("paracetamol_500mg_moins1_qc").cast(num)
                + pl.col("paracetamol_500mg_1a3ans_qc").cast(num)
                + pl.col("paracetamol_500mg_3ansplus_qc").cast(num)
            ).alias(
                "paracetamol_500mg_moins1_qc_plus_paracetamol_500mg_1a3ans_qc_plus_paracetamol_500mg_3ansplus_qc"
            ),
            (
                pl.col("casorientcs-5_f").cast(num)
                + pl.col("casorientcs-5_g").cast(num)
            ).alias("casorientcs-5_f_plus_casorientcs-5_g"),
            (
                pl.col("casorientcs5plus_f").cast(num)
                + pl.col("casorientcs5plus_g").cast(num)
            ).alias("casorientcs5plus_f_plus_casorientcs5plus_g"),
            (
                pl.col("cascontreorient-5_f").cast(num)
                + pl.col("cascontreorient-5_g").cast(num)
            ).alias("cascontreorient-5_f_plus_cascontreorient-5_g"),
            (
                pl.col("cascontreorient5plus_f").cast(num)
                + pl.col("cascontreorient5plus_g").cast(num)
            ).alias("cascontreorient5plus_f_plus_cascontreorient5plus_g"),
            (
                pl.col("casprestb-5_f").cast(num) + pl.col("casprestb-5_g").cast(num)
            ).alias("casprestb-5_f_plus_casprestb-5_g"),
            (
                pl.col("cassusppalugrav-5_f").cast(num)
                + pl.col("cassusppalugrav-5_g").cast(num)
            ).alias("cassusppalugrav-5_f_plus_cassusppalugrav-5_g"),
            (
                pl.col("cassusppalugrav5plus_f").cast(num)
                + pl.col("cassusppalugrav5plus_g").cast(num)
            ).alias("cassusppalugrav5plus_f_plus_cassusppalugrav5plus_g"),
            (
                pl.col("castbconfirm-5_f").cast(num)
                + pl.col("castbconfirm-5_g").cast(num)
            ).alias("castbconfirm-5_f_plus_castbconfirm-5_g"),
            (
                pl.col("caseffetindes-5_f").cast(num)
                + pl.col("caseffetindes-5_g").cast(num)
                + pl.col("caseffetindes5plus_f").cast(num)
                + pl.col("caseffetindes5plus_g").cast(num)
            ).alias(
                "caseffetindes-5_f_plus_caseffetindes-5_g_plus_caseffetindes5plus_f_plus_caseffetindes5plus_g"
            ),
            (
                pl.col("pbrouge6-59_f").cast(num)
                + pl.col("pbrouge6-59_g").cast(num)
                + pl.col("pbjaune6-59_f").cast(num)
                + pl.col("pbjaune6-59_g").cast(num)
            ).alias(
                "pbrouge6-59_f_plus_pbrouge6-59_g_plus_pbjaune6-59_f_plus_pbjaune6-59_g"
            ),
        ]
    )

    return data.with_columns(
        [
            yes_no_to_int("minuteur_ari", "count yes minuteur_ari"),
            yes_no_to_int("minuteur_ari", "count yes minuteur_ari 2"),
            yes_no_to_int("muac", "count yes muac"),
            yes_no_to_int("muac", "count yes muac 2"),
            yes_no_to_int("poubelle_recep", "count yes poubelle_recep"),
            yes_no_to_int("caissemedexiste", "count yes caissemedexiste"),
            yes_no_to_int("caissesecure", "count yes caissesecure"),
        ]
    )


def authenticate_iaso(conn: IASOConnection) -> IASO:
    """Authenticates and returns an IASO object.

    Args:
        conn (IASOConnection): IASO connection details.

    Returns:
        IASO: An authenticated IASO object.
    """
    try:
        iaso = IASO(conn.url, conn.username, conn.password)
        current_run.log_info("IASO authentication successful")
        return iaso
    except Exception as exc:
        error_msg = f"IASO authentication failed: {exc}"
        current_run.log_error(error_msg)
        raise RuntimeError(error_msg) from exc


def get_form_name(iaso_connection: IASO, form_id: int) -> str:
    """Retrieve and sanitize form name.

    Args:
        iaso_connection (IASO): An authenticated IASO object.
        form_id (int): The ID of the form to check.

    Returns:
        str: Form name.

    Raises:
        ValueError: If the form does not exist.
    """
    iaso = authenticate_iaso(iaso_connection)
    try:
        response = iaso.api_client.get(
            f"/api/forms/{form_id}", params={"fields": {"name"}}
        )
        return clean_string(response.json().get("name"))
    except Exception as e:
        current_run.log_error(f"Form fetch failed: {e}")
        raise ValueError("Invalid form ID") from e


def parse_cutoff_date(date_str: str | None) -> str | None:
    """Validate and parse ISO date string.

    Args:
        date_str: Input date string in YYYY-MM-DD format

    Returns:
        Validated date string or None

    Raises:
        ValueError: For invalid date formats
    """
    if not date_str:
        return None

    try:
        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError as exc:
        current_run.log_error("Invalid date format - must be YYYY-MM-DD")
        raise ValueError("Invalid date format") from exc


def fetch_submissions(
    iaso: IASO,
    form_id: int,
    cutoff_date: str | None,
) -> pl.DataFrame:
    """Retrieve form submissions from IASO API.

    Args:
        iaso: Authenticated IASO client
        form_id: Target form identifier
        cutoff_date: Optional date filter

    Returns:
        DataFrame containing form submissions
    """
    try:
        current_run.log_info(f"Fetching submissions for form ID {form_id}")
        csv = dataframe._get_instances_csv(iaso, form_id, last_updated=cutoff_date)
        return pl.read_csv(
            StringIO(csv),
            ignore_errors=True,
            infer_schema_length=10000,
        )
    except Exception as exc:
        current_run.log_error(f"Submission retrieval failed: {exc}")
        raise


def process_choices(
    submissions: pl.DataFrame, convert: bool, iaso_client: IASO, form_id: int
) -> pl.DataFrame:
    """Convert choice codes to human-readable labels if requested.

    Args:
        submissions: Raw submissions DataFrame
        convert: Conversion flag
        iaso_client: Authenticated IASO client
        form_id: Target form identifier

    Returns:
        Processed DataFrame with labels if requested
    """
    if not convert:
        return submissions

    try:
        form_metadata = dataframe.get_form_metadata(iaso_client, form_id)
        return dataframe.replace_labels(
            submissions=submissions, form_metadata=form_metadata, language="French"
        )
    except Exception as exc:
        current_run.log_error(f"Choice conversion failed: {exc}")
        raise


def deduplicate_columns(submissions: pl.DataFrame) -> pl.DataFrame:
    """Renames duplicate columns in the DataFrame by appending a unique suffix.

    Args:
        submissions: DataFrame with potential duplicate columns.

    Returns:
        pl.DataFrame: DataFrame with unique column names.
    """
    cleaned_columns = [clean_string(col) for col in submissions.columns]
    duplicates_columns = {
        k: list(range(1, cleaned_columns.count(k) + 1))
        for k in cleaned_columns
        if cleaned_columns.count(k) != 1
    }
    for col in cleaned_columns[:]:  # Iterate over a copy of the list
        if col in duplicates_columns:
            index = cleaned_columns.index(col)
            cleaned_columns[index] = f"{col}_{duplicates_columns[col][0]}"
            duplicates_columns[col].remove(duplicates_columns[col][0])

    submissions.columns = cleaned_columns
    return _process_submissions(submissions)


def export_to_file(
    submissions: pl.DataFrame,
    form_name: str,
    output_file_name: str,
    output_format: str,
    raw: bool,
    transformed_file_path: str = ""
) -> Path:
    """Export submissions data to specified file format.

    Args:
        submissions: DataFrame containing the form submissions.
        form_name: Name of the form to use in the output file name.
        output_file_name: Optional custom output file name. If not provided, defaults to
            `iaso-pipelines/extract-submissions/form_<form_name>.<output_format>`.
        output_format: File format extension for the output file.
        raw: Bolean to determine whether to write files in raw or transformed directories
        transformed_file_path: str path to the transformed file

    Returns:
        Path: The path to the exported file.
    """
    if raw:
        periods = submissions["periode" if raw else "period"].unique()
        for period in periods:
            file_name = (
                f"pipelines/sanru-iaso-to-dhis2/raw/{output_file_name}_{period}"
            )
            output_file_path = _generate_output_file_path(
                form_name=form_name,
                output_file_name=file_name,
                output_format=output_format,
            )
            current_run.log_info(
                f"Exporting submissions fiile to: `{output_file_path}`"
            )
            period_submission = submissions.filter(
                pl.col("periode" if raw else "period") == period
            )
            if output_format == ".csv":
                period_submission.write_csv(output_file_path)
            elif output_format == ".parquet":
                period_submission.write_parquet(output_file_path)
            else:
                period_submission.to_pandas().to_excel(output_file_path, index=False)

            # fpath = output_file_path.as_posix()
            # current_run.add_file_output(fpath)
    else:
        file_name = f"{transformed_file_path}"
        output_file_path = _generate_output_file_path(
            form_name=form_name,
            output_file_name=file_name,
            output_format=output_format,
        )
        if output_format == ".csv":
            submissions.write_csv(output_file_path)
        elif output_format == ".parquet":
            submissions.write_parquet(output_file_path)
        else:
            submissions.to_pandas().to_excel(output_file_path, index=False)

        # fpath = output_file_path.as_posix()
        # current_run.add_file_output(fpath)
    return output_file_path


def post_to_dhis2(
    pusher: DHIS2Pusher,
    transformed_data: pl.DataFrame,
    dry_run: bool,
) -> dict[str, Any]:
    """Post transformed data to target DHIS2 instance."""
    current_run.log_info(f"Posting data to target DHIS2 (dry_run={dry_run})...")

    if len(transformed_data) == 0:
        current_run.log_warning("No data to post after transformation")

    try:
        # Prepare payload
        payload = prepare_data_value_payload(transformed_data)
        current_run.log_info(f"Prepared {len(payload)} data values for posting")
        # Post data
        pusher.push_data(df_data=payload.to_pandas())
        # Log results
        current_run.log_info("✓ Data posting completed")

    except Exception as e:

        current_run.log_error(f"Error posting data: {e}")
        raise


def prepare_data_value_payload(data_values: pl.DataFrame) -> list[dict[str, Any]]:
    """Prepare data values for DHIS2 API payload.

    Returns:
        list[dict[str, Any]]: List of data value dictionaries for API.
    """
    mapping_toolbox_dhis2_name = {
        "DX_UID": "dataElement",
        "ORG_UNIT": "orgUnit",
        "PERIOD": "period",
        "CATEGORY_OPTION_COMBO": "categoryOptionCombo",
        "VALUE": "value",
        "DATA_TYPE": "dataType",
        "ATTRIBUTE_OPTION_COMBO": "attributeOptionCombo"
    }

    # Check for required columns
    missing_columns = [
        col for col in mapping_toolbox_dhis2_name if col not in data_values.columns
    ]
    if missing_columns:
        current_run.log_error(f"Missing required columns: {missing_columns}")
        raise ValueError(f"Missing required columns: {missing_columns}")
    data_values = data_values.rename(mapping_toolbox_dhis2_name)
    # Convert to dictionaries
    return data_values.select(list(mapping_toolbox_dhis2_name.values()))


def _process_submissions(submissions: pl.DataFrame) -> pl.DataFrame:
    """Process and clean the submissions DataFrame.

    Returns:
        pl.DataFrame: The cleaned and processed submissions DataFrame.
    """
    list_cols = submissions.select(pl.col(pl.List(pl.Utf8))).columns

    binary_exprs = []
    for col in list_cols:
        unique_cats = (
            submissions[col].drop_nulls().explode().drop_nulls().unique().to_list()
        )

        for cat in unique_cats:
            expr = (
                pl.col(col)
                .list.contains(cat)
                .fill_null(False)
                .cast(pl.Int8)
                .alias(f"{col}_{clean_string(cat)}")
            )
            binary_exprs.append(expr)

    if binary_exprs:
        submissions = submissions.with_columns(binary_exprs)

    submissions = submissions.drop(list_cols).select(
        pl.exclude("instanceid"), pl.col("instanceid")
    )

    return submissions.select(sorted(submissions.columns)).sort(submissions.columns)


def _generate_output_file_path(
    form_name: str, output_file_name: str, output_format: str
) -> Path:
    """Generate the output file path based on provided parameters.

    Args:
        form_name: Name of the form to include in the file name.
        output_file_name: Optional custom output file name.
        output_format: File format extension for the output file.

    Returns:
        Path to the output file.
    """
    if output_file_name:
        output_file_path = Path(output_file_name)

        if not output_file_path.suffix:
            output_file_path = output_file_path.with_suffix(output_format)

        if output_file_path.suffix not in [".csv", ".parquet", ".xlsx"]:
            current_run.log_error(
                f"Unsupported output format: {output_file_path.suffix}. "
                "Supported formats are: .csv, .parquet, .xlsx"
            )
            raise ValueError(f"Unsupported output format: {output_file_path.suffix}")

        if not output_file_path.is_absolute():
            output_file_path = Path(workspace.files_path, output_file_path)

        output_file_path.parent.mkdir(parents=True, exist_ok=True)
        return output_file_path

    output_dir = Path(workspace.files_path, "iaso-pipelines", "extract-submissions")
    output_dir.mkdir(exist_ok=True, parents=True)

    base_name = f"{clean_string(form_name)}"
    timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M")
    file_name = f"{base_name}_{timestamp}{output_format}"

    return output_dir / file_name


def clean_string(input_str: str) -> str:
    """Normalize and sanitize string for safe file/table names.

    Args:
        input_str: Original input string

    Returns:
        Normalized string with special characters removed
    """
    normalized = unicodedata.normalize("NFD", input_str)
    cleaned = "".join(c for c in normalized if not unicodedata.combining(c))
    sanitized = CLEAN_PATTERN.sub("", cleaned)
    return sanitized.strip().replace(" ", "_").lower()


if __name__ == "__main__":
    sanru_iaso_to_dhis2()
