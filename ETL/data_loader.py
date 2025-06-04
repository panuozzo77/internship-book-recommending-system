# ETL/data_loader.py
import pandas as pd
import logging
import gzip
import os

logger = logging.getLogger(__name__)


def _resolve_source_path(relative_path: str, base_dir: str):
    """Resolves a source path, assuming it's relative to base_dir if not absolute."""
    if os.path.isabs(relative_path):
        return relative_path
    return os.path.join(base_dir, relative_path)


def load_single_source_to_dataframe(source_config: dict, global_etl_settings: dict, raw_datasets_base_dir: str):
    """
    Loads a single source file into a Pandas DataFrame.

    Args:
        source_config (dict): Configuration for the source file (path, format, alias, etc.).
        global_etl_settings (dict): Global settings from the ETL config (e.g., sample_n_rows).
        raw_datasets_base_dir (str): The base directory for raw datasets if paths are relative.

    Returns:
        pd.DataFrame or None: The loaded DataFrame, or None on failure.
    """
    path_from_config = source_config['path']
    fmt = source_config['format']
    alias = source_config['alias']
    sample_n = global_etl_settings.get("sample_n_rows")  # from ETL global settings

    # Resolve path: from ETL config, paths are relative to raw_datasets_base_dir (from main config)
    # The etl_runner should have already made source paths absolute.
    # This function now expects source_config['path'] to be absolute.
    absolute_path = path_from_config

    logger.info(f"Loading source '{alias}' from '{absolute_path}' (format: {fmt}, sample_n: {sample_n})")

    if not os.path.exists(absolute_path):
        logger.error(f"File not found for source '{alias}': {absolute_path}")
        return None

    try:
        df = None
        opener = gzip.open if absolute_path.endswith(".gz") else open
        read_mode = 'rt' if absolute_path.endswith(".gz") else 'r'  # text mode for gzip

        if fmt == "json_lines":
            # Pandas read_json with lines=True handles gzipped files automatically if path is given
            # but to explicitly use our opener for consistency:
            with opener(absolute_path, read_mode, encoding='utf-8') as f:
                df = pd.read_json(f, lines=True, nrows=sample_n)
        elif fmt == "csv":
            # Pandas read_csv handles gzipped files automatically if path is given
            # but to explicitly use our opener:
            with opener(absolute_path, read_mode, encoding='utf-8') as f:
                df = pd.read_csv(f, nrows=sample_n)
        else:
            logger.error(f"Unsupported format '{fmt}' for source '{alias}'.")
            return None

        # Rename columns if specified
        if 'columns_to_rename' in source_config and isinstance(source_config['columns_to_rename'], dict):
            df.rename(columns=source_config['columns_to_rename'], inplace=True)
            logger.debug(f"Renamed columns for '{alias}' as per config.")

        logger.info(f"Successfully loaded {len(df)} rows into DataFrame '{alias}'. Columns: {df.columns.tolist()}")
        return df
    except FileNotFoundError:  # Should be caught by os.path.exists, but as a safeguard
        logger.error(f"File not found during pandas read for source '{alias}': {absolute_path}")
    except pd.errors.EmptyDataError:
        logger.warning(f"Source file '{alias}' at '{absolute_path}' is empty or contains only headers.")
        return pd.DataFrame()  # Return empty DataFrame
    except Exception as e:
        logger.error(f"Error loading source '{alias}' from '{absolute_path}': {e}", exc_info=True)
    return None


def load_all_sources(etl_mapping_config: dict):
    """
    Loads all defined sources in the ETL mapping configuration into a dictionary of DataFrames.
    Assumes paths in etl_mapping_config.sources have been made absolute by the caller (etl_runner).

    Args:
        etl_mapping_config (dict): The loaded ETL mapping configuration.

    Returns:
        dict: A dictionary where keys are source aliases and values are Pandas DataFrames.
    """
    dataframes = {}
    global_etl_settings = etl_mapping_config.get("global_settings", {})
    # raw_datasets_base_dir is not needed here if paths are already absolute

    for source_conf in etl_mapping_config.get("sources", []):
        if 'path' not in source_conf or 'format' not in source_conf or 'alias' not in source_conf:
            logger.warning(f"Skipping invalid source configuration: {source_conf}")
            continue
        # Path is expected to be absolute here, resolved by etl_runner
        df = load_single_source_to_dataframe(source_conf, global_etl_settings, "")  # Pass empty base_dir
        if df is not None:  # Could be an empty DataFrame, which is fine
            dataframes[source_conf['alias']] = df
        else:
            logger.error(
                f"Failed to load source '{source_conf['alias']}'. It will not be available for joins or targets.")
            # Optionally, you could make this a critical failure
            # return {} # or raise an exception

    return dataframes