from dataclasses import dataclass

from soda.common.logs import Logs
from soda.sampler.sample import Sample
from soda.soda_cloud.soda_cloud import SodaCloud


@dataclass
class SampleContext:

    sample: Sample
    sample_name: str
    query: str
    logs: Logs
    soda_cloud: SodaCloud
