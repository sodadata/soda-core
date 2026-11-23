"""Entra workload identity federation for Azure Databricks (``auth_type: azure-workload-identity``).

The runner pod's projected Kubernetes service-account token is exchanged at Entra ID for an access
token of a user-assigned managed identity, with ``azure.identity.WorkloadIdentityCredential``. The
access token is handed to ``databricks-sql-connector`` through its ``credentials_provider``.

The environment resolution, credential construction and Entra error mapping mirror
``soda_sqlserver.common.data_sources.sqlserver_data_source_connection`` (including the ``tenant_id``
override of sodadata/soda-core#3010). They are duplicated rather than shared because soda-databricks
does not depend on soda-sqlserver; moving both onto one module in soda-core is a follow-up.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, Optional

from soda_core.common.exceptions import DataSourceConnectionException
from soda_databricks.model.data_source.databricks_connection_properties import DatabricksAzureWorkloadIdentity

# The Azure Databricks first-party application ID: the only resource a workload identity token is
# requested for. It is not configurable, so no setting can mint a token for Key Vault, ARM, Storage or
# any other resource the managed identity may also have access to.
AZURE_DATABRICKS_TOKEN_SCOPE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"

WORKLOAD_IDENTITY_SETUP_HINT = (
    "Label the runner's pod with 'azure.workload.identity/use: \"true\"' so that the Azure workload identity "
    "webhook injects it, or mount the projected service account token and set AZURE_TENANT_ID, AZURE_CLIENT_ID "
    "and AZURE_FEDERATED_TOKEN_FILE in the runner's Helm values."
)
CONNECTION_IDENTITY_HINT = (
    "A single connection can instead set tenant_id and client_id itself, for a managed identity of its own or one "
    "in another Entra tenant."
)
# Entra's error for a client ID that has no service principal in the tenant the token was requested from.
AADSTS_APPLICATION_NOT_FOUND = "AADSTS700016"
CROSS_TENANT_HINT = (
    "The managed identity was not found in tenant '{tenant_id}'. If it lives in another Entra tenant, set "
    "tenant_id on the connection to that tenant's ID."
)
CONFIGURED_TENANT_HINT = (
    "The managed identity was not found in tenant '{tenant_id}', which is set as tenant_id on the connection. "
    "Check that tenant_id and client_id on the connection are correct and belong to the same Entra tenant."
)


@dataclass(frozen=True)
class WorkloadIdentity:
    tenant_id: str
    client_id: str
    token_file_path: str


def resolve_workload_identity(config: DatabricksAzureWorkloadIdentity) -> WorkloadIdentity:
    """Checks the pod environment before any credential is built, so that a pod that is not set up for
    workload identity fails with the missing variable and the Kubernetes fix, not a bare ValueError.
    The YAML tenant_id and client_id win over AZURE_TENANT_ID and AZURE_CLIENT_ID. The token file is the pod's own
    service account token, the same for every tenant, so it always comes from the environment."""
    tenant_id = config.tenant_id or os.environ.get("AZURE_TENANT_ID")
    token_file_path = os.environ.get("AZURE_FEDERATED_TOKEN_FILE")
    client_id = config.client_id or os.environ.get("AZURE_CLIENT_ID")
    missing = [
        name
        for name, value in [
            ("AZURE_TENANT_ID (or tenant_id in the connection)", tenant_id),
            ("AZURE_FEDERATED_TOKEN_FILE", token_file_path),
            ("AZURE_CLIENT_ID (or client_id in the connection)", client_id),
        ]
        if not value
    ]
    if missing:
        verb = "is" if len(missing) == 1 else "are"
        # The token file can only come from the pod; the tenant and client ID can also come from the connection.
        connection_hint = f" {CONNECTION_IDENTITY_HINT}" if not tenant_id or not client_id else ""
        raise DataSourceConnectionException(
            f"Workload identity federation is not set up for this pod: {', '.join(missing)} "
            f"{verb} not set. {WORKLOAD_IDENTITY_SETUP_HINT}{connection_hint}"
        )
    return WorkloadIdentity(tenant_id=tenant_id, client_id=client_id, token_file_path=token_file_path)


class WorkloadIdentityHeaderFactory:
    """The header factory handed to databricks-sql-connector, which calls it for every HTTP request.

    Each call asks the credential for a token rather than reusing one fetched at connect time:
    azure-identity caches the access token and only goes back to Entra when it is close to expiry, so
    a long scan keeps working after the first token expires.

    The connector wraps any exception raised from here in its own generic ``RequestError``. The last
    Entra rejection is therefore kept on ``last_error`` so the connection layer can surface it as is.
    """

    def __init__(self, credential, identity: WorkloadIdentity, tenant_id_configured: bool):
        self._credential = credential
        self.identity = identity
        self._tenant_id_configured = tenant_id_configured
        self.last_error: Optional[DataSourceConnectionException] = None

    def __call__(self) -> Dict[str, str]:
        from azure.core.exceptions import ClientAuthenticationError

        try:
            token = self._credential.get_token(AZURE_DATABRICKS_TOKEN_SCOPE).token
        except ClientAuthenticationError as e:
            self.last_error = entra_rejection(e, self.identity, self._tenant_id_configured)
            raise self.last_error from e
        self.last_error = None
        return {"Authorization": f"Bearer {token}"}


def entra_rejection(error, identity: WorkloadIdentity, tenant_id_configured: bool) -> DataSourceConnectionException:
    # Entra's message carries the AADSTS code and description (wrong issuer, subject or audience on the
    # federated credential, unknown client ID, ...); it never contains the assertion itself.
    entra_message = getattr(error, "message", None) or str(error)
    message = (
        f"Entra ID rejected the workload identity token exchange for client ID '{identity.client_id}' "
        f"in tenant '{identity.tenant_id}': {entra_message}"
    )
    if AADSTS_APPLICATION_NOT_FOUND in entra_message:
        hint = CONFIGURED_TENANT_HINT if tenant_id_configured else CROSS_TENANT_HINT
        message = f"{message} {hint.format(tenant_id=identity.tenant_id)}"
    return DataSourceConnectionException(message)


def build_workload_identity_header_factory(config: DatabricksAzureWorkloadIdentity) -> WorkloadIdentityHeaderFactory:
    # Imported here so that azure.identity is only loaded by the mode that needs it.
    from azure.identity import WorkloadIdentityCredential

    identity = resolve_workload_identity(config)
    # Construct WorkloadIdentityCredential explicitly, never DefaultAzureCredential, so a scan pod has no
    # credential chain to fall through. AZURE_AUTHORITY_HOST still comes from the environment.
    credential = WorkloadIdentityCredential(
        tenant_id=identity.tenant_id,
        client_id=identity.client_id,
        token_file_path=identity.token_file_path,
    )
    return WorkloadIdentityHeaderFactory(credential, identity, tenant_id_configured=config.tenant_id is not None)


def databricks_rejection(
    error: BaseException, config: DatabricksAzureWorkloadIdentity, identity: WorkloadIdentity
) -> Optional[DataSourceConnectionException]:
    """Returns a diagnostic error when Databricks answered 401/403 to a request that carried a workload
    identity token, else None."""
    http_code = _http_code(error)
    if http_code not in (401, 403):
        return None
    return DataSourceConnectionException(
        f"An Entra access token was acquired through workload identity federation, but Databricks workspace "
        f"'{config.host}' rejected it with HTTP {http_code}: {error}. Check that the managed identity "
        f"(client ID '{identity.client_id}') is added to the workspace as a service principal, and that it has "
        f"the Unity Catalog privileges and CAN USE on the SQL warehouse that Soda needs."
    )


def _http_code(error: BaseException) -> Optional[int]:
    # databricks-sql-connector puts the HTTP status in RequestError.context["http-code"].
    seen = set()
    current: Optional[BaseException] = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        context = getattr(current, "context", None)
        if isinstance(context, dict):
            code = context.get("http-code")
            if isinstance(code, int):
                return code
        current = current.__cause__ or current.__context__
    return None
