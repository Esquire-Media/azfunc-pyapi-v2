from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Iterable, Literal, Sequence
import json
import os
import time
import uuid

import orjson
import pandas as pd
from azure.core.exceptions import AzureError
from azure.storage.blob import (
    BlobClient,
    BlobSasPermissions,
    ContainerClient,
    ContainerSasPermissions,
    generate_blob_sas,
    generate_container_sas,
)
from dateutil.relativedelta import relativedelta

from libs.data import from_bind, register_binding
from libs.openapi.clients.onspot import OnSpot


PullType = Literal["devices", "demographics", "observations"]
AddressPullType = Literal["devices"]


_ENDPOINTS: dict[str, str] = {
    "devices": "/save/geoframe/all/devices",
    "demographics": "/save/geoframe/demographics/all",
    "observations": "/save/geoframe/all/observations",
}

_ADDRESS_ENDPOINTS: dict[str, str] = {
    "devices": "/save/addresses/all/devices",
}


class OneOffError(RuntimeError):
    pass


class ConfigError(OneOffError):
    pass


class InputError(OneOffError):
    pass


class RequestError(OneOffError):
    pass


class PollTimeoutError(OneOffError):
    pass


@dataclass(slots=True)
class Ingress:
    instance_id: str
    conn_str_env: str
    container_name: str
    output_path: str
    endpoint: str

    def as_dict(self) -> dict[str, str]:
        return {
            "instance_id": self.instance_id,
            "conn_str_env": self.conn_str_env,
            "container": self.container_name,
            "outputPath": self.output_path,
            "endpoint": self.endpoint,
        }


def load_local_settings(path: str | os.PathLike[str] = "local.settings.json") -> dict[str, Any]:
    settings_path = Path(path)
    if not settings_path.is_file():
        raise ConfigError(f"Settings file not found: {settings_path}")

    try:
        payload = orjson.loads(settings_path.read_bytes())
    except Exception as exc:
        raise ConfigError(f"Invalid JSON in settings file: {settings_path}") from exc

    values = payload.get("Values")
    if not isinstance(values, dict):
        raise ConfigError("local.settings.json must contain a top-level 'Values' object.")

    os.environ.update({k: str(v) for k, v in values.items()})
    return values


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ConfigError(f"Required environment variable is missing: {name}")
    return value


def _validate_event_url(url: str) -> str:
    value = str(url).strip()
    if not value:
        raise InputError("event_url is required.")
    if not value.startswith(("http://", "https://")):
        raise InputError(f"event_url must be http(s): {value}")
    return value


def _validate_event_url_template(template: str) -> str:
    value = str(template).strip()
    if not value:
        raise InputError("event_url_template is required.")
    if not value.startswith(("http://", "https://")):
        raise InputError(f"event_url_template must be http(s): {value}")
    if "{eventName}" not in value:
        raise InputError("event_url_template must include '{eventName}'.")
    return value


def _get_default_event_url() -> str:
    return _validate_event_url("https://webhook.site/...")


def _get_default_event_url_template() -> str:
    return _validate_event_url_template("https://webhook.site/.../{eventName}")


def ensure_keystone_binding() -> tuple[Any, dict[str, Any], Any]:
    register_binding(
        "keystone",
        "Structured",
        "sql",
        url=_require_env("DATABIND_SQL_KEYSTONE"),
        schemas={"keystone": None},
    )
    provider = from_bind("keystone")
    tables = provider.models["keystone"]
    session = provider.connect()
    return provider, tables, session


def _normalize_ids(ids: Sequence[str] | str) -> list[str]:
    values = [ids] if isinstance(ids, str) else list(ids)
    normalized = [str(x).strip() for x in values if str(x).strip()]
    if not normalized:
        raise InputError("At least one ESQID is required.")
    return normalized


def fetch_targeting_geoframes(
    esqids: Sequence[str] | str,
    *,
    session: Any | None = None,
    tables: dict[str, Any] | None = None,
) -> list[Any]:
    ids = _normalize_ids(esqids)
    owns_session = session is None or tables is None

    if owns_session:
        _, tables, session = ensure_keystone_binding()

    try:
        rows = list(
            session.query(
                tables["TargetingGeoFrame"].id,
                tables["TargetingGeoFrame"].ESQID,
                tables["TargetingGeoFrame"].polygon,
            ).filter(tables["TargetingGeoFrame"].ESQID.in_(ids))
        )
    except Exception as exc:
        raise RequestError("Failed to fetch targeting geoframes from keystone.") from exc
    finally:
        if owns_session:
            try:
                session.close()
            except Exception:
                pass

    found_ids = {str(row.ESQID) for row in rows}
    missing = [esqid for esqid in ids if esqid not in found_ids]
    if missing:
        raise InputError(f"No geoframe found for ESQID(s): {missing}")

    return rows


def create_ingress(
    *,
    pull_type: PullType | AddressPullType,
    mode: Literal["geoframe", "addresses"],
    instance_id: str | None = None,
    conn_str_env: str | None = None,
    container_name: str | None = None,
    output_prefix: str = "oneoff",
) -> Ingress:
    if mode == "geoframe":
        endpoint = _ENDPOINTS[pull_type]
    elif mode == "addresses":
        endpoint = _ADDRESS_ENDPOINTS[pull_type]
    else:
        raise InputError("mode must be 'geoframe' or 'addresses'.")

    resolved_instance_id = instance_id or uuid.uuid4().hex
    resolved_conn_str_env = conn_str_env or (
        "ONSPOT_CONN_STR" if os.environ.get("ONSPOT_CONN_STR") else "AzureWebJobsStorage"
    )
    resolved_container_name = container_name or os.environ.get("ONSPOT_CONTAINER", "dashboard")

    if not os.environ.get(resolved_conn_str_env):
        if resolved_conn_str_env != "AzureWebJobsStorage" and not os.environ.get("AzureWebJobsStorage"):
            raise ConfigError(
                f"Storage connection string env var is missing: {resolved_conn_str_env} "
                "and AzureWebJobsStorage is also not set."
            )
        if resolved_conn_str_env == "AzureWebJobsStorage":
            raise ConfigError("Storage connection string env var is missing: AzureWebJobsStorage")

    return Ingress(
        instance_id=resolved_instance_id,
        conn_str_env=resolved_conn_str_env,
        container_name=resolved_container_name,
        output_path=f"{output_prefix}/{resolved_instance_id}/{pull_type}",
        endpoint=endpoint,
    )


def get_container_client(ingress: Ingress) -> ContainerClient:
    conn_str = os.environ.get(ingress.conn_str_env) or os.environ.get("AzureWebJobsStorage")
    if not conn_str:
        raise ConfigError(
            f"Neither {ingress.conn_str_env} nor AzureWebJobsStorage is set."
        )

    try:
        container = ContainerClient.from_connection_string(
            conn_str,
            container_name=ingress.container_name,
        )
        if not container.exists():
            container.create_container()
        return container
    except AzureError as exc:
        raise ConfigError(
            f"Unable to access blob container '{ingress.container_name}'."
        ) from exc


def generate_container_az_url(
    container: ContainerClient,
    prefix: str,
    *,
    expiry_days: int = 2,
) -> str:
    sas_token = generate_container_sas(
        account_name=container.account_name,
        account_key=container.credential.account_key,
        container_name=container.container_name,
        permission=ContainerSasPermissions(write=True, read=True, list=True),
        expiry=datetime.utcnow() + relativedelta(days=expiry_days),
    )
    return f"{container.url.replace('https://', 'az://')}/{prefix}?{sas_token}"


def generate_blob_az_url(
    container: ContainerClient,
    blob_name: str,
    *,
    read: bool = True,
    write: bool = False,
    expiry_days: int = 2,
) -> str:
    sas = generate_blob_sas(
        account_name=container.account_name,
        container_name=container.container_name,
        blob_name=blob_name,
        account_key=container.credential.account_key,
        permission=BlobSasPermissions(read=read, write=write),
        expiry=datetime.utcnow() + relativedelta(days=expiry_days),
    )
    return (
        f"az://{container.account_name}.blob.core.windows.net/"
        f"{container.container_name}/{blob_name}?{sas}"
    )


def build_geoframe_requests(
    rows: Sequence[Any],
    *,
    start: datetime,
    end: datetime,
    hash_values: bool = False,
    name_field: Literal["id", "ESQID"] = "id",
) -> list[dict[str, Any]]:
    if start >= end:
        raise InputError("start must be earlier than end.")

    requests: list[dict[str, Any]] = []

    for row in rows:
        polygon = row.polygon or {}
        features = [
            {
                **feature,
                "properties": {
                    "name": str(getattr(row, name_field)),
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "hash": hash_values,
                },
            }
            for feature in polygon.get("features", [])
        ]
        if features:
            requests.append({"type": "FeatureCollection", "features": features})

    if not requests:
        raise InputError("No valid geoframe features were found for the requested ESQIDs.")

    return requests


def attach_geoframe_output_metadata(
    requests: list[dict[str, Any]],
    *,
    ingress: Ingress,
    event_url_template: str,
) -> tuple[ContainerClient, list[dict[str, Any]]]:
    validated_template = _validate_event_url_template(event_url_template)
    container = get_container_client(ingress)
    output_location = generate_container_az_url(container, ingress.output_path)

    for request in requests:
        for feature in request.get("features", []):
            props = feature.setdefault("properties", {})
            props["callback"] = validated_template.replace("{eventName}", uuid.uuid4().hex)
            props["outputLocation"] = output_location

    return container, requests


def validate_address_file(local_path: str | os.PathLike[str]) -> Path:
    path = Path(local_path).expanduser()

    if not path.is_file():
        raise InputError(f"Address file not found: {path}")
    if path.suffix.lower() != ".csv":
        raise InputError(f"Address file must be a .csv file: {path}")
    if path.stat().st_size == 0:
        raise InputError(f"Address file is empty: {path}")

    try:
        sample = pd.read_csv(path, nrows=5)
    except Exception as exc:
        raise InputError(f"Address file could not be read as CSV: {path}") from exc

    required_columns = {"address", "city", "state", "zipCode"}
    missing_columns = sorted(required_columns - set(sample.columns))
    if missing_columns:
        raise InputError(
            f"Address CSV missing required columns: {missing_columns}. "
            "Expected at least: address, city, state, zipCode"
        )

    return path


def upload_local_file_as_source(
    local_path: str | os.PathLike[str],
    *,
    ingress: Ingress | None = None,
    blob_name: str | None = None,
) -> tuple[ContainerClient, str, str]:
    path = validate_address_file(local_path)
    resolved_ingress = ingress or create_ingress(pull_type="devices", mode="addresses")
    container = get_container_client(resolved_ingress)

    resolved_blob_name = blob_name or f"oneoff/{resolved_ingress.instance_id}/inputs/{path.name}"
    blob = container.get_blob_client(resolved_blob_name)

    try:
        with path.open("rb") as f:
            blob.upload_blob(f, overwrite=True)
    except AzureError as exc:
        raise RequestError(f"Failed to upload source file to blob storage: {path}") from exc

    https_url = blob.url
    az_url = generate_blob_az_url(container, resolved_blob_name, read=True, write=False)
    return container, https_url, az_url


def build_address_requests(
    source_urls: Sequence[str] | str,
    *,
    match_acceptance_threshold: float = 29.9,
    hash_values: bool = False,
) -> list[dict[str, Any]]:
    urls = [source_urls] if isinstance(source_urls, str) else list(source_urls)
    normalized_urls = [str(url).strip() for url in urls if str(url).strip()]

    if not normalized_urls:
        raise InputError("At least one source URL is required for address requests.")

    return [
        {
            "hash": hash_values,
            "name": uuid.uuid4().hex,
            "fileName": f"{uuid.uuid4().hex}.csv",
            "fileFormat": {"delimiter": ",", "quoteEncapsulate": True},
            "mappings": {
                "street": ["address"],
                "city": ["city"],
                "state": ["state"],
                "zip": ["zipCode"],
                "zip4": ["plus4Code"],
            },
            "matchAcceptanceThreshold": match_acceptance_threshold,
            "sources": [url],
        }
        for url in normalized_urls
    ]


def attach_address_output_metadata(
    requests: list[dict[str, Any]],
    *,
    ingress: Ingress,
    event_url: str,
) -> tuple[ContainerClient, list[dict[str, Any]]]:
    validated_url = _validate_event_url(event_url)
    container = get_container_client(ingress)
    output_location = generate_container_az_url(container, ingress.output_path)

    for request in requests:
        request["outputLocation"] = output_location
        request["callback"] = validated_url

    return container, requests


def submit_requests(requests: Sequence[dict[str, Any]], *, ingress: Ingress) -> list[Any]:
    if not requests:
        raise InputError("No requests were generated to submit.")

    req = OnSpot[(ingress.endpoint, "post")]
    responses: list[Any] = []

    for index, payload in enumerate(requests, start=1):
        try:
            response = req(payload)
            print(payload)
        except Exception as exc:
            raise RequestError(
                f"Request {index}/{len(requests)} failed for endpoint {ingress.endpoint}"
            ) from exc

        if not response:
            raise RequestError(
                f"Request {index}/{len(requests)} returned an empty response from {ingress.endpoint}"
            )

        responses.append(response)

    return responses


def iter_output_blobs(
    container: ContainerClient,
    *,
    prefix: str,
    results_per_page: int = 100,
) -> Iterable[list[Any]]:
    blobs = container.list_blobs(name_starts_with=f"{prefix}/")

    try:
        pager = blobs.by_page(results_per_page=results_per_page)
    except TypeError:
        try:
            blobs = container.list_blobs(
                name_starts_with=f"{prefix}/",
                results_per_page=results_per_page,
            )
            pager = blobs.by_page()
        except TypeError:
            yield list(container.list_blobs(name_starts_with=f"{prefix}/"))
            return

    for page in pager:
        yield list(page)


def list_output_blob_names(
    container: ContainerClient,
    *,
    prefix: str,
    results_per_page: int = 100,
    exclude_debug: bool = False,
) -> list[str]:
    return [
        blob.name
        for page in iter_output_blobs(container, prefix=prefix, results_per_page=results_per_page)
        for blob in page
        if _should_include_blob(blob.name, exclude_debug=exclude_debug)
    ]


def get_output_blob_count(
    container: ContainerClient,
    *,
    prefix: str,
    exclude_debug: bool = False,
) -> int:
    return len(
        list_output_blob_names(
            container,
            prefix=prefix,
            exclude_debug=exclude_debug,
        )
    )


def _read_blob_to_frame(blob_client: BlobClient, *, blob_name: str) -> pd.DataFrame:
    content = blob_client.download_blob().readall()
    suffix = Path(blob_name).suffix.lower()

    if suffix in {".csv", ".txt"}:
        df = pd.read_csv(BytesIO(content))
    elif suffix == ".json":
        payload = json.loads(content)
        if isinstance(payload, list):
            df = pd.DataFrame(payload)
        elif isinstance(payload, dict):
            df = pd.json_normalize(payload)
        else:
            raise InputError(f"Unsupported JSON payload in blob: {blob_name}")
    elif suffix == ".jsonl":
        df = pd.read_json(BytesIO(content), lines=True)
    elif suffix == ".parquet":
        df = pd.read_parquet(BytesIO(content))
    else:
        try:
            df = pd.read_csv(BytesIO(content))
        except Exception as exc:
            raise InputError(f"Unsupported blob format for {blob_name}") from exc

    df["source_blob"] = blob_name
    return df


def read_output_blobs(
    container: ContainerClient,
    *,
    prefix: str,
    results_per_page: int = 100,
    exclude_debug: bool = False,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    errors: list[str] = []

    for page in iter_output_blobs(container, prefix=prefix, results_per_page=results_per_page):
        for blob in page:
            if not _should_include_blob(blob.name, exclude_debug=exclude_debug):
                continue
            try:
                blob_client = container.get_blob_client(blob.name)
                frames.append(_read_blob_to_frame(blob_client, blob_name=blob.name))
            except Exception as exc:
                errors.append(f"{blob.name}: {exc}")

    if frames:
        return pd.concat(frames, ignore_index=True)

    if errors:
        raise OneOffError("Failed to read output blobs:\n" + "\n".join(errors))

    return pd.DataFrame()

def _is_debug_blob(blob_name: str) -> bool:
    return "debug" in Path(blob_name).name.lower()

def _should_include_blob(
    blob_name: str,
    *,
    exclude_debug: bool = False,
) -> bool:
    if exclude_debug and _is_debug_blob(blob_name):
        return False
    return True

def wait_for_output_blobs(
    container: ContainerClient,
    *,
    prefix: str,
    expected_at_least: int = 1,
    timeout_seconds: int = 900,
    poll_seconds: int = 15,
    verbose: bool = True,
    label: str = "Polling output blobs",
    exclude_debug: bool = False,
) -> int:
    deadline = time.time() + timeout_seconds
    last_count = -1

    if verbose:
        print(f"{label}...")
        print(f"  prefix: {prefix}")
        print(f"  found:  0 / {expected_at_least}")

    while time.time() < deadline:
        current_count = get_output_blob_count(
            container,
            prefix=prefix,
            exclude_debug=exclude_debug,
        )

        if verbose and current_count != last_count:
            print(f"  found: {current_count:>2} / {expected_at_least}")

        if current_count >= expected_at_least:
            if verbose:
                print("Completed.")
            return current_count

        last_count = current_count
        time.sleep(poll_seconds)

    final_count = get_output_blob_count(
        container,
        prefix=prefix,
        exclude_debug=exclude_debug,
    )

    if verbose:
        print(f"Timed out. Final count: {final_count} / {expected_at_least}")

    raise PollTimeoutError(
        f"Timed out waiting for output blobs under '{prefix}'. "
        f"Expected at least {expected_at_least}, found {final_count}."
    )


def pull_by_esqids(
    esqids: Sequence[str] | str,
    *,
    start: datetime,
    end: datetime,
    pull_type: PullType = "devices",
    event_url_template: str | None = None,
    wait: bool = False,
    timeout_seconds: int = 900,
    verbose: bool = True,
) -> dict[str, Any]:
    resolved_event_url_template = (
        _validate_event_url_template(event_url_template)
        if event_url_template is not None
        else _get_default_event_url_template()
    )

    rows = fetch_targeting_geoframes(esqids)
    requests = build_geoframe_requests(rows, start=start, end=end)
    ingress = create_ingress(pull_type=pull_type, mode="geoframe")
    container, requests = attach_geoframe_output_metadata(
        requests,
        ingress=ingress,
        event_url_template=resolved_event_url_template,
    )
    responses = submit_requests(requests, ingress=ingress)

    blob_count: int | None = None
    if wait:
        expected = len(requests)
        blob_count = wait_for_output_blobs(
            container,
            prefix=ingress.output_path,
            expected_at_least=expected,
            timeout_seconds=timeout_seconds,
            verbose=verbose,
            label=f"Polling {pull_type} results for ESQID pull",
        )

    return {
        "ingress": ingress,
        "request_count": len(requests),
        "responses": responses,
        "blob_count": blob_count,
        "blob_prefix": ingress.output_path,
        "requested_esqids": _normalize_ids(esqids),
    }


def pull_by_address_file(
    local_path: str | os.PathLike[str],
    *,
    pull_type: AddressPullType = "devices",
    event_url: str | None = None,
    wait: bool = False,
    timeout_seconds: int = 900,
    verbose: bool = True,
) -> dict[str, Any]:
    resolved_event_url = (
        _validate_event_url(event_url)
        if event_url is not None
        else _get_default_event_url()
    )

    ingress = create_ingress(pull_type=pull_type, mode="addresses")
    container, _, source_az_url = upload_local_file_as_source(local_path, ingress=ingress)
    requests = build_address_requests(source_az_url)
    container, requests = attach_address_output_metadata(
        requests,
        ingress=ingress,
        event_url=resolved_event_url,
    )
    responses = submit_requests(requests, ingress=ingress)

    blob_count: int | None = None
    if wait:
        blob_count = wait_for_output_blobs(
            container,
            prefix=ingress.output_path,
            expected_at_least=len(requests),
            timeout_seconds=timeout_seconds,
            verbose=verbose,
            label=f"Polling {pull_type} results for address pull",
            exclude_debug=True,
        )

    return {
        "ingress": ingress,
        "request_count": len(requests),
        "responses": responses,
        "blob_count": blob_count,
        "blob_prefix": ingress.output_path,
        "local_path": str(Path(local_path).expanduser()),
        "exclude_debug_blobs": True,
    }


def get_results_frame(
    result: dict[str, Any],
    *,
    fail_if_empty: bool = True,
    exclude_debug: bool | None = None,
) -> pd.DataFrame:
    ingress = result["ingress"]
    container = get_container_client(ingress)

    resolved_exclude_debug = (
        result.get("exclude_debug_blobs", False)
        if exclude_debug is None
        else exclude_debug
    )

    df = read_output_blobs(
        container,
        prefix=ingress.output_path,
        exclude_debug=resolved_exclude_debug,
    )

    if fail_if_empty and df.empty:
        raise OneOffError(f"No output files found under blob prefix: {ingress.output_path}")

    return df