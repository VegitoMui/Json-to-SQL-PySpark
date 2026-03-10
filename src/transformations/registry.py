from src.transformations.select_transform import (
    handle_select_sql,
    handle_select_pyspark,
)

from src.transformations.filter_transform import (
    handle_filter_sql,
    handle_filter_pyspark,
)

from src.transformations.join_transform import (
    handle_join_sql,
    handle_join_pyspark,
)

from src.transformations.groupby_transform import (
    handle_groupby_sql,
    handle_groupby_pyspark,
)

from src.transformations.case_transform import (
    handle_case_sql,
    handle_case_pyspark,
)

from src.transformations.column_transform import (
    handle_column_transform_sql,
    handle_column_transform_pyspark,
)

from src.transformations.order_transform import (
    handle_order_sql,
    handle_order_pyspark,
)

from src.transformations.limit_transform import (
    handle_limit_sql,
    handle_limit_pyspark,
)

from src.transformations.window_transform import (
    handle_window_sql,
    handle_window_pyspark,
)


SQL_TRANSFORM_REGISTRY = {
    "select": handle_select_sql,
    "filter": handle_filter_sql,
    "join": handle_join_sql,
    "groupby": handle_groupby_sql,
    "case_when": handle_case_sql,
    "column_transform": handle_column_transform_sql,
    "order_by": handle_order_sql,
    "limit": handle_limit_sql,
    "window": handle_window_sql,
}


PYSPARK_TRANSFORM_REGISTRY = {
    "select": handle_select_pyspark,
    "filter": handle_filter_pyspark,
    "join": handle_join_pyspark,
    "groupby": handle_groupby_pyspark,
    "case_when": handle_case_pyspark,
    "column_transform": handle_column_transform_pyspark,
    "order_by": handle_order_pyspark,
    "limit": handle_limit_pyspark,
    "window": handle_window_pyspark,
}