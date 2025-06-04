# ETL/data_joiner.py
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def perform_joins(dataframes_dict: dict, join_configs: list):
    """
    Performs join operations on DataFrames as defined in the join_configs.
    Modifies dataframes_dict in place by adding new joined DataFrames.

    Args:
        dataframes_dict (dict): Dictionary of DataFrames, keyed by alias.
        join_configs (list): List of join configuration dictionaries.

    Returns:
        dict: The updated dictionary of DataFrames (same object as input).
    """
    if not join_configs:
        logger.info("No join operations defined.")
        return dataframes_dict

    for i, join_conf in enumerate(join_configs):
        left_alias = join_conf.get('left_df_alias')
        right_alias = join_conf.get('right_df_alias')
        result_alias = join_conf.get('result_alias')
        left_on = join_conf.get('left_on')
        right_on = join_conf.get('right_on')  # Can be same as left_on if column names match
        how = join_conf.get('how', 'inner')  # Default to inner join
        suffixes_conf = join_conf.get('suffixes', ['_x', '_y'])  # Default Pandas suffixes

        if not all([left_alias, right_alias, result_alias, left_on]):
            logger.error(f"Skipping join config at index {i} due to missing required fields "
                         f"(left_df_alias, right_df_alias, result_alias, left_on): {join_conf}")
            continue

        right_on = right_on or left_on  # If right_on not specified, assume same as left_on

        if left_alias not in dataframes_dict:
            logger.error(
                f"Cannot perform join '{result_alias}': Left DataFrame '{left_alias}' not found. Available: {list(dataframes_dict.keys())}")
            continue
        if right_alias not in dataframes_dict:
            logger.error(
                f"Cannot perform join '{result_alias}': Right DataFrame '{right_alias}' not found. Available: {list(dataframes_dict.keys())}")
            continue

        left_df = dataframes_dict[left_alias]
        right_df = dataframes_dict[right_alias]

        logger.info(f"Performing join '{result_alias}': '{left_alias}' ({len(left_df)} rows) "
                    f"{how.upper()} JOIN '{right_alias}' ({len(right_df)} rows) "
                    f"ON {left_alias}.{left_on} = {right_alias}.{right_on}")
        try:
            # Ensure join keys are of compatible types or handle conversions if necessary
            # Basic check for column existence
            if left_on not in left_df.columns:
                logger.error(
                    f"Join key '{left_on}' not found in left DataFrame '{left_alias}'. Columns: {left_df.columns.tolist()}")
                continue
            if right_on not in right_df.columns:
                logger.error(
                    f"Join key '{right_on}' not found in right DataFrame '{right_alias}'. Columns: {right_df.columns.tolist()}")
                continue

            joined_df = pd.merge(
                left_df,
                right_df,
                left_on=left_on,
                right_on=right_on,
                how=how,
                suffixes=tuple(suffixes_conf)  # Ensure suffixes is a tuple
            )
            dataframes_dict[result_alias] = joined_df
            logger.info(f"Join '{result_alias}' successful. Resulting DataFrame has {len(joined_df)} rows "
                        f"and columns: {joined_df.columns.tolist()}")
        except Exception as e:
            logger.error(f"Error performing join '{result_alias}': {e}", exc_info=True)
            # Optionally, decide if this error should halt further joins or the ETL

    return dataframes_dict