import argparse
from dganalytics.utils.utils import exec_powerbi_refresh, get_secret, get_logger

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--tenant', required=True)
    args, unknown_args = parser.parse_known_args()
    tenant = args.tenant
    workspace_id = get_secret(f"{tenant}PowerBiWorkspaceId")
    dataset_id = get_secret(f"{tenant}PowerBiDatasetId")
    exec_powerbi_refresh(workspace_id, dataset_id)
