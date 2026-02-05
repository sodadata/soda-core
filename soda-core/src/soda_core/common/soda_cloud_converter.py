from soda_core.common import soda_cloud_dto, sql_ast


def map_sampler_type_from_dto(sampler_type_dto: soda_cloud_dto.SamplerType) -> sql_ast.SamplerType:
    if sampler_type_dto is None:
        return None
    elif sampler_type_dto == soda_cloud_dto.SamplerType.ABSOLUTE_LIMIT:
        return sql_ast.SamplerType.ABSOLUTE_LIMIT
    else:
        raise ValueError(f"Unsupported sampler type DTO: {sampler_type_dto}")
