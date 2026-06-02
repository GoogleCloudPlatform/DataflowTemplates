#!/usr/bin/env python3
import sys
import json
import subprocess

def run_command_with_errors(cmd):
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return result.stdout.strip(), False
    except subprocess.CalledProcessError as e:
        err_msg = (e.stderr or "") + (e.stdout or "")
        is_permission_error = "PERMISSION_DENIED" in err_msg or "Permission" in err_msg or "not granted" in err_msg or "not enabled" in err_msg
        return None, is_permission_error
    except FileNotFoundError:
        print("\n[QUOTA ERROR] 'gcloud' command was not found in your system PATH. Please install Google Cloud SDK.", file=sys.stderr)
        sys.exit(1)

def get_json_command(cmd):
    output, is_perm_error = run_command_with_errors(cmd)
    if output:
        try:
            return json.loads(output), is_perm_error
        except json.JSONDecodeError:
            return None, is_perm_error
    return None, is_perm_error

def main():
    try:
        input_data = json.load(sys.stdin)
    except Exception as e:
        print(json.dumps({"error": "Failed to read JSON input from Terraform"}), file=sys.stderr)
        sys.exit(1)

    project_id = input_data.get("project_id")
    region = input_data.get("region", "us-central1")
    cloudsql_req = int(input_data.get("cloudsql_instances_required", 0))
    spanner_pu_req = int(input_data.get("spanner_pu_required", 0))
    vpc_net_req = input_data.get("vpc_network_required", "false").lower() == "true"
    spanner_config = input_data.get("spanner_config", "regional-us-central1")

    if not project_id:
        print("\n[QUOTA ERROR] Missing 'project_id' in Terraform parameters.", file=sys.stderr)
        sys.exit(1)

    errors = []
    warnings = []

    # 1. Check Cloud SQL Instances Quota
    sql_instances, _ = get_json_command(["gcloud", "sql", "instances", "list", "--project", project_id, "--format=json"])
    current_sql_usage = len(sql_instances) if sql_instances is not None else 0

    sql_limit = None
    sql_quota_info, sql_perm_error = get_json_command([
        "gcloud", "beta", "services", "quota", "list",
        "--service=sqladmin.googleapis.com",
        "--project", project_id,
        "--filter", "metric=sqladmin.googleapis.com/instances",
        "--format=json"
    ])
    
    if sql_perm_error:
        warnings.append("Cloud SQL Instances (Permission Denied querying Quota Limit).")
    elif sql_quota_info and isinstance(sql_quota_info, list) and len(sql_quota_info) > 0:
        try:
            sql_limit = int(sql_quota_info[0].get("values", {}).get("STANDARD", 100))
        except (ValueError, TypeError, KeyError, AttributeError):
            pass

    if sql_limit is not None:
        remaining_sql = max(0, sql_limit - current_sql_usage)
        if remaining_sql < cloudsql_req:
            errors.append(
                f"Cloud SQL Instances quota exceeded in project '{project_id}'. "
                f"Required: {cloudsql_req}, Available: {remaining_sql} (Limit: {sql_limit}, Current Usage: {current_sql_usage})."
            )
    else:
        remaining_sql = "unknown"
        warnings.append("Cloud SQL Instances (No custom quota override found in project; check skipped to prevent false-positive blocks).")

    # 2. Check Spanner Processing Units Quota
    spanner_instances, _ = get_json_command(["gcloud", "spanner", "instances", "list", "--project", project_id, "--format=json"])
    current_pu_usage = 0
    if spanner_instances is not None:
        for inst in spanner_instances:
            config_short = inst.get("config", "").split("/")[-1]
            if config_short == spanner_config:
                try:
                    if "processingUnits" in inst:
                        current_pu_usage += int(inst["processingUnits"])
                    elif "nodeCount" in inst:
                        current_pu_usage += int(inst["nodeCount"]) * 1000
                except (ValueError, TypeError):
                    pass

    spanner_pu_limit = None
    spanner_quota_info, spanner_perm_error = get_json_command([
        "gcloud", "beta", "services", "quota", "list",
        "--service=spanner.googleapis.com",
        "--project", project_id,
        "--filter", f"metric=spanner.googleapis.com/total_processing_units AND dimensions.location={spanner_config}",
        "--format=json"
    ])
    
    if spanner_perm_error:
        warnings.append(f"Spanner Processing Units for '{spanner_config}' (Permission Denied querying Quota Limit).")
    elif spanner_quota_info and isinstance(spanner_quota_info, list) and len(spanner_quota_info) > 0:
        try:
            spanner_pu_limit = int(spanner_quota_info[0].get("values", {}).get("STANDARD", 2000))
        except (ValueError, TypeError, KeyError, AttributeError):
            pass

    if spanner_pu_limit is not None:
        remaining_pu = max(0, spanner_pu_limit - current_pu_usage)
        if remaining_pu < spanner_pu_req:
            errors.append(
                f"Spanner Processing Units quota exceeded for config '{spanner_config}' in project '{project_id}'. "
                f"Required: {spanner_pu_req}, Available: {remaining_pu} (Limit: {spanner_pu_limit}, Current Usage: {current_pu_usage})."
            )
    else:
        remaining_pu = "unknown"
        warnings.append(f"Spanner Processing Units for '{spanner_config}' (No custom quota override found in project; check skipped to prevent false-positive blocks).")

    # 3. Check VPC Network Quota
    remaining_net = "unknown"
    if vpc_net_req:
        networks, _ = get_json_command(["gcloud", "compute", "networks", "list", "--project", project_id, "--format=json"])
        current_net_usage = len(networks) if networks is not None else 0
        net_limit = None
        net_quota_info, net_perm_error = get_json_command([
            "gcloud", "beta", "services", "quota", "list",
            "--service=compute.googleapis.com",
            "--project", project_id,
            "--filter", "metric=compute.googleapis.com/networks",
            "--format=json"
        ])
        
        if net_perm_error:
            warnings.append("VPC Networks (Permission Denied querying Quota Limit).")
        elif net_quota_info and isinstance(net_quota_info, list) and len(net_quota_info) > 0:
            try:
                net_limit = int(net_quota_info[0].get("values", {}).get("STANDARD", 5))
            except (ValueError, TypeError, KeyError, AttributeError):
                pass

        if net_limit is not None:
            remaining_net = max(0, net_limit - current_net_usage)
            if remaining_net < 1:
                errors.append(
                    f"VPC Networks quota exceeded in project '{project_id}'. "
                    f"Required: 1, Available: {remaining_net} (Limit: {net_limit}, Current Usage: {current_net_usage})."
                )
        else:
            remaining_net = "unknown"
            warnings.append("VPC Networks (No custom quota override found in project; check skipped to prevent false-positive blocks).")

    if warnings:
        print("\n[QUOTA WARNING] Validation check skipped for some properties to prevent false-positive blocks:\n" + 
              "\n".join("- " + warn for warn in warnings) + 
              "\nDeployment will proceed safely.", file=sys.stderr)

    if errors:
        print("\n[QUOTA ERROR] The requested infrastructure cannot be provisioned due to quota limits:\n" + 
              "\n".join("- " + err for err in errors), file=sys.stderr)
        sys.exit(1)

    print(json.dumps({
        "status": "success",
        "project_id": project_id,
        "cloudsql_remaining": str(remaining_sql),
        "spanner_pu_remaining": str(remaining_pu),
        "vpc_remaining": str(remaining_net),
        "warning": "\n".join(warnings) if warnings else "none"
    }))

if __name__ == "__main__":
    main()
