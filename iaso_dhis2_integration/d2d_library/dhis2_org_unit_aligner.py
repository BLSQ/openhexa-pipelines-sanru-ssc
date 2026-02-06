import json
import logging

import pandas as pd
import requests
from openhexa.sdk import current_run
from openhexa.toolbox.dhis2 import DHIS2
from requests import Response
from requests.structures import CaseInsensitiveDict

from .org_unit_point import OrgUnitObj


class DHIS2PyramidAligner:
    """Align organisation units between two DHIS2 instances.

    This class is stateless and provides methods to synchronize organisation units
    from a source DHIS2 instance to a target DHIS2 instance. The alignment process
    compares the pyramids of both instances and performs the necessary operations
    to keep the target up to date with the source.

    Supported operations include:
    - Creating organisation units that exist in the source but not in the target.
    - Updating organisation units that exist in both but differ in their attributes.

    This class does not store any state between calls; all data must be provided
    as method parameters.
    """

    def __init__(self, logger: logging.Logger):
        self.logger = logger if logger else logging.getLogger(__name__)

    def align_to(
        self,
        target_dhis2: DHIS2,
        source_pyramid: pd.DataFrame,
        dry_run: bool = True,
    ):
        """Syncs the extracted pyramid data with the target DHIS2 instance."""
        # Load the target pyramid
        if source_pyramid.empty:
            current_run.log_warning("Source pyramid is empty. Organisation units alignment skipped.")
            return

        current_run.log_debug(f"Shape source pyramid: {source_pyramid.shape}")
        current_run.log_info(f"Retrieving organisation units from target DHIS2: {target_dhis2.api.url}")
        # Retrieve all organisation units from the target DHIS2
        target_pyramid = target_dhis2.meta.organisation_units(
            fields="id,name,shortName,openingDate,closedDate,parent,level,path,geometry"
        )
        target_pyramid = pd.DataFrame(target_pyramid)
        current_run.log_debug(f"Shape target pyramid: {target_pyramid.shape}")
        current_run.log_info(f"Run org units sync with dry_run: {dry_run}")

        # Select new OU: all OU in source not in target (set difference)
        ou_new = list(set(source_pyramid.id) - set(target_pyramid.id))
        ou_to_create = source_pyramid[source_pyramid.id.isin(ou_new)]
        self._push_org_units_create(
            ou_to_create=ou_to_create,
            target_dhis2=target_dhis2,
            dry_run=dry_run,
        )

        # Select matching OU: all OU uid that match between DHIS2 source and target (set intersection)
        matching_ou_ids = list(set(source_pyramid.id).intersection(set(target_pyramid.id)))
        self._push_org_units_update(
            org_unit_source=source_pyramid,
            org_unit_target=target_pyramid,
            ou_ids_to_check=matching_ou_ids,
            target_dhis2=target_dhis2,
            dry_run=dry_run,
        )
        current_run.log_info("Organisation units push finished.")

    def _push_org_units_create(self, ou_to_create: pd.DataFrame, target_dhis2: DHIS2, dry_run: bool) -> None:
        """Create organisation units in the target DHIS2 instance.

        Parameters
        ----------
        ou_to_create : pd.DataFrame
            DataFrame containing organisation unit data to be created.
        target_dhis2 : DHIS2
            DHIS2 client for the target instance.
        dry_run : bool
            If True, performs a dry run without making changes.
        report_path : str
            Path to store the execution report.

        This function iterates over the organisation units, validates them, and
        attempts to create them in the target DHIS2.
        Logs errors and information about the creation process.
        """
        if not ou_to_create.shape[0] > 0:
            current_run.log_info("No new organisation units to create.")
            return

        try:
            # NOTE: Geometry is valid for versions > 2.32
            if target_dhis2.version <= "2.32":
                ou_to_create["geometry"] = None
                current_run.log_warning("DHIS2 version not compatible with geometry. Geometry will not be pushed.")

            current_run.log_info(f"Creating {len(ou_to_create)} organisation units.")
            errors_count = 0
            for row_tuple in ou_to_create.itertuples(index=False, name="OrgUnitRow"):
                ou = OrgUnitObj(row_tuple)
                if ou.is_valid():
                    response = self._push_org_unit(
                        dhis2_client=target_dhis2,
                        org_unit=ou,
                        strategy="CREATE",
                        dry_run=dry_run,  # dry_run=False -> Apply changes in the DHIS2
                    )
                    if response["status"] == "ERROR":
                        errors_count = errors_count + 1
                        self.logger.info(str(response))
                    else:
                        current_run.log_info(f"New organisation unit created: {ou}")
                        self.logger.info(f"New organisation unit created: {ou}")
                else:
                    self.logger.info(
                        str(
                            {
                                "action": "CREATE",
                                "statusCode": None,
                                "status": "NOTVALID",
                                "response": None,
                                "ou_id": row_tuple.id,
                            }
                        )
                    )

            if errors_count > 0:
                current_run.log_info(
                    f"{errors_count} errors occurred during creation. Please check the latest execution logs."
                )

        except Exception as e:
            raise Exception(f"Unexpected error occurred while creating organisation units. Error: {e}") from e

    def _push_org_units_update(
        self,
        org_unit_source: pd.DataFrame,
        org_unit_target: pd.DataFrame,
        ou_ids_to_check: list[str],
        target_dhis2: DHIS2,
        dry_run: bool,
    ):
        """Update org units based on matching id list."""
        if not len(ou_ids_to_check) > 0:
            current_run.log_info("No organisation units to update.")
            return

        try:
            current_run.log_info(f"Checking for updates in {len(ou_ids_to_check)} organisation units.")
            # NOTE: Geometry is valid for versions > 2.32
            if target_dhis2.version <= "2.32":
                org_unit_source["geometry"] = None
                org_unit_target["geometry"] = None
                current_run.log_warning("DHIS2 version not compatible with geometry. Geometry will be ignored.")

            # build id dictionary (faster) to compare source vs target OU
            index_dictionary = self._build_id_indexes(org_unit_source, org_unit_target, ou_ids_to_check)

            errors_count = 0
            updates_count = 0
            progress_count = 0
            for _, indices in index_dictionary.items():
                progress_count = progress_count + 1

                # Create the OU and check if there are differences
                # NOTE: See OrgUnitObj._eq_() to check the comparison logic
                ou_source = OrgUnitObj(org_unit_source.iloc[indices["source"]])
                ou_target = OrgUnitObj(org_unit_target.iloc[indices["target"]])

                if ou_source != ou_target:
                    response = self._push_org_unit(
                        dhis2_client=target_dhis2,
                        org_unit=ou_source,
                        strategy="UPDATE",
                        dry_run=dry_run,  # dry_run=False -> Apply changes in the DHIS2
                        is_testing=False,
                    )
                    if response["status"] == "ERROR":
                        errors_count = errors_count + 1
                    else:
                        updates_count = updates_count + 1
                    self.logger.info(str(response))

                if progress_count % 5000 == 0:
                    current_run.log_info(f"Organisation units checked: {progress_count}/{len(ou_ids_to_check)}")

            current_run.log_info(f"Organisation units updated: {updates_count}")
            if errors_count > 0:
                current_run.log_info(
                    f"{errors_count} errors occurred during OU update. Please check the latest execution logs."
                )
        except Exception as e:
            raise Exception(f"Unexpected error occurred while updating organisation units. Error: {e}") from e

    def _push_org_unit(
        self,
        dhis2_client: DHIS2,
        org_unit: OrgUnitObj,
        strategy: str = "CREATE",
        dry_run: bool = True,
        is_testing: bool = False,
    ) -> dict:
        """Pushes an organisation unit to the DHIS2 instance using the specified strategy.

        Parameters
        ----------
        dhis2_client : DHIS2
            DHIS2 client for the target instance.
        org_unit : OrgUnitObj
            Organisation unit object to be pushed.
        strategy : str, optional
            Strategy for pushing ('CREATE' or 'UPDATE'), by default "CREATE".
        dry_run : bool, optional
            If True, performs a dry run without making changes, by default True.
        is_testing : bool, optional
            If True, runs the function in test mode, by default False.

        Returns
        -------
        dict
            Formatted response from the DHIS2 API.
        """
        if is_testing:
            response = {"importCount": {"imported": 1, "ignored": 0}}
            payload = {"status": "OK", "response": response}
            r = Response()
            r.status_code = 200
            r.headers = CaseInsensitiveDict({"Content-Type": "application/json"})
            r._content = json.dumps().encode("utf-8")  # private attr used internally
        else:
            if strategy == "CREATE":
                endpoint = "organisationUnits"
                payload = org_unit.to_json()

            if strategy == "UPDATE":
                endpoint = "metadata"
                payload = {"organisationUnits": [org_unit.to_json()]}

            r = dhis2_client.api.session.post(
                f"{dhis2_client.api.url}/{endpoint}",
                json=payload,
                params={"dryRun": dry_run, "importStrategy": f"{strategy}"},
            )

        return self._build_formatted_response(response=r, strategy=strategy, ou_id=org_unit.id)

    def _build_formatted_response(self, response: requests.Response, strategy: str, ou_id: str) -> dict:
        """Build a formatted response dictionary from a requests.Response object.

        Parameters
        ----------
        response : requests.Response
            The HTTP response object from the requests library.
        strategy : str
            The strategy or action performed.
        ou_id : str
            The organisational unit ID related to the response.

        Returns
        -------
        dict
            A dictionary containing the action, status code, status, response, and organisational unit ID.
        """
        return {
            "action": strategy,
            "statusCode": response.status_code,
            "status": response.json().get("status"),
            "response": response.json().get("response"),
            "ou_id": ou_id,
        }

    def _build_id_indexes(self, ou_source: pd.DataFrame, ou_target: pd.DataFrame, ou_matching_ids: list) -> dict:
        """Build a dictionary mapping matching OU IDs to their index positions in source and target DataFrames.

        Parameters
        ----------
        ou_source : pd.DataFrame
            Source DataFrame containing organisation units with an 'id' column.
        ou_target : pd.DataFrame
            Target DataFrame containing organisation units with an 'id' column.
        ou_matching_ids : list
            List of organisation unit IDs to match between source and target.

        Returns
        -------
        dict
            Dictionary where keys are matching IDs and values are dicts with 'source' and 'target' index positions.
        """
        # Set "id" as the index for faster lookup
        df1_lookup = {val: idx for idx, val in enumerate(ou_source["id"])}
        df2_lookup = {val: idx for idx, val in enumerate(ou_target["id"])}

        # Build the dictionary using prebuilt lookups
        return {
            match_id: {"source": df1_lookup[match_id], "target": df2_lookup[match_id]}
            for match_id in ou_matching_ids
            if match_id in df1_lookup and match_id in df2_lookup
        }
