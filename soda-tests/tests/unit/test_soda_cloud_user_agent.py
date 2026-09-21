"""Every request to Soda Cloud carries a User-Agent that names the client stack.

Soda Cloud reads the product tokens to tell soda-core apart from other clients, so a
request that drops the header (because it sets its own Authorization or Content-Type)
would be indistinguishable from an unknown client.
"""

from unittest import mock

import pytest
from soda_core.__version__ import SODA_CORE_VERSION
from soda_core.common import user_agent as user_agent_module
from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.user_agent import register_user_agent_product, user_agent
from soda_core.common.yaml import SodaCloudYamlSource

YAML_SOURCE: SodaCloudYamlSource = SodaCloudYamlSource.from_str(
    """
soda_cloud:
  host: dev.sodadata.io
  api_key_id: some_key_id
  api_key_secret: some_key_secret
"""
)


@pytest.fixture(autouse=True)
def isolated_registry(monkeypatch):
    monkeypatch.setattr(user_agent_module, "_products", list(user_agent_module._products))


def _soda_cloud() -> SodaCloud:
    soda_cloud = SodaCloud.from_yaml_source(YAML_SOURCE, provided_variable_values={})
    soda_cloud.token = "some_token"
    return soda_cloud


def test_user_agent_is_soda_core_by_default():
    assert user_agent() == f"soda-core/{SODA_CORE_VERSION}"


def test_registered_products_follow_soda_core():
    register_user_agent_product("soda-extensions", "4.24.0")
    assert user_agent() == f"soda-core/{SODA_CORE_VERSION} soda-extensions/4.24.0"


def test_registering_a_product_again_replaces_its_version():
    register_user_agent_product("soda-extensions", "4.24.0")
    register_user_agent_product("soda-extensions", "4.25.0")
    assert user_agent() == f"soda-core/{SODA_CORE_VERSION} soda-extensions/4.25.0"


@pytest.mark.parametrize("name, version", [("soda extensions", "1.0"), ("soda-extensions", "1.0 beta"), ("", "1.0")])
def test_invalid_product_tokens_are_rejected(name, version):
    with pytest.raises(ValueError):
        register_user_agent_product(name, version)


def test_headers_reflect_products_registered_after_construction():
    soda_cloud = _soda_cloud()
    register_user_agent_product("soda-extensions", "4.24.0")
    assert soda_cloud.headers == {"User-Agent": f"soda-core/{SODA_CORE_VERSION} soda-extensions/4.24.0"}


def test_request_headers_keep_user_agent_next_to_request_specific_headers():
    soda_cloud = _soda_cloud()
    headers = soda_cloud.request_headers({"Authorization": "some_token", "Content-Type": "application/json"})
    assert headers == {
        "User-Agent": f"soda-core/{SODA_CORE_VERSION}",
        "Authorization": "some_token",
        "Content-Type": "application/json",
    }


@mock.patch("requests.post")
def test_log_batch_upload_sends_user_agent(mock_post):
    mock_post.return_value = mock.Mock(status_code=200)
    soda_cloud = _soda_cloud()

    soda_cloud._post_log_batch(url="https://dev.sodadata.io/api/logs/x/batch", body="{}", request_log_name="logs")

    assert mock_post.call_args.kwargs["headers"]["User-Agent"] == f"soda-core/{SODA_CORE_VERSION}"


@mock.patch("requests.get")
def test_rest_get_sends_user_agent(mock_get):
    mock_get.return_value = mock.Mock(status_code=200, headers={})
    soda_cloud = _soda_cloud()

    soda_cloud._execute_rest_get(relative_url_path="datasets", request_log_name="datasets")

    assert mock_get.call_args.kwargs["headers"]["User-Agent"] == f"soda-core/{SODA_CORE_VERSION}"
