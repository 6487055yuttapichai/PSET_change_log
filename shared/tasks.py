from datetime import timedelta
from json import JSONDecodeError
from typing import Tuple
import pandas as pd
import json
from .tdm_logging import logger, log_error


class Tasks:
    str_class = 'process_bypasses'
    def __init__(self):
        pass

    def process_bypasses(self, df: pd.DataFrame):
        mod_str = 'process_bypasses'
        def process_fail_reason(json_str):
            mod_str = 'process_fail_reason'
            try:
                data = json.loads(json_str)

                # Extract fail_reason
                fail_reason = data.get('fail reason', None)

                # Extract supervisor_override
                rework_confirmed_by = data.get('reworkConfirmedBy', {})
                supervisor_name = rework_confirmed_by.get('name', '')
                badge_id = rework_confirmed_by.get('badgeId', '')
                supervisor_override = f"{supervisor_name} ({badge_id})" if supervisor_name and badge_id else None

                # Extract route_to
                functions = rework_confirmed_by.get('functions', [])
                route_to = [func for func in functions if func not in ["supervisor", "operator"]]
                route_to = ', '.join(route_to) if route_to else None

                # Convert override_timestamp to datetime
                record_time = rework_confirmed_by.get('recordTime', None)
                override_timestamp = pd.to_datetime(record_time) if record_time else None

                return fail_reason, supervisor_override, route_to, override_timestamp
            except (JSONDecodeError, KeyError, TypeError, ValueError) as err:
                log_error(self.str_class, mod_str, type(err).__name__, str(err))
                return None, None, None, None
            except Exception as e:
                log_error(self.str_class, mod_str, 'Exception', str(e))
                return None, None, None, None

        def process_manual_override(json_str):
            mod_str = 'process_fail_reason'
            try:
                data = json.loads(json_str)

                # Extract supervisor_override
                manually_confirmed_by = data.get('manuallyCompletedBy', None)
                supervisor_name = manually_confirmed_by.get('name', '')
                badge_id = manually_confirmed_by.get('badgeId', '')
                supervisor_override = f"{supervisor_name} ({badge_id})" if supervisor_name and badge_id else None

                # Convert override timestamp to datetime
                record_time = manually_confirmed_by.get('recordTime', None)
                override_timestamp = pd.to_datetime(record_time) if record_time else None
                return supervisor_override, override_timestamp
            except (JSONDecodeError, KeyError, TypeError, ValueError) as err:
                log_error(self.str_class, mod_str, type(err).__name__, str(err))
                return None, None
            except Exception as e:
                log_error(self.str_class, mod_str, 'Exception', str(e))
                return None, None

        try:
            task_data = df.copy()
            task_data['fail_reason'] = None
            task_data['supervisor_override'] = None
            task_data['route_to'] = None
            task_data['override_timestamp'] = None

            df_task_data = (task_data[(task_data['task_status'] != 'FAILED') &
                                      (task_data['task_status'] != 'COMPLETED (MANUALLY)')]
                            .reset_index())
            df_task_data.drop(columns=['extra_data_json', 'index'], inplace=True)
            df_task_data = df_task_data.fillna('')
            # df_task_data['override_timestamp'] = pd.to_datetime(df_task_data['override_timestamp'], utc=True)

            df_failed_tasks = task_data[task_data['task_status'] == 'FAILED']
            if df_failed_tasks is not None and not df_failed_tasks.empty:
                (df_failed_tasks['fail_reason'], df_failed_tasks['supervisor_override'],
                 df_failed_tasks['route_to'], df_failed_tasks['override_timestamp']) = zip(*df_failed_tasks['extra_data_json']
                                                                                           .apply(process_fail_reason))
                df_failed_tasks = df_failed_tasks.drop(columns=['extra_data_json']).fillna('')
                df_task_data = pd.concat([df_task_data, df_failed_tasks], ignore_index=True)

            # Apply function to parse manual bypass input
            df_bypassed_tasks = task_data[task_data['task_status'] == 'COMPLETED (MANUALLY)']
            if df_bypassed_tasks is not None and not df_bypassed_tasks.empty:
                (df_bypassed_tasks['supervisor_override'],
                 df_bypassed_tasks['override_timestamp']) = zip(*df_bypassed_tasks['extra_data_json']
                                                                .apply(process_manual_override))
                df_bypassed_tasks = df_bypassed_tasks.drop(columns=['extra_data_json']).fillna('')
                df_task_data = pd.concat([df_task_data, df_bypassed_tasks], ignore_index=True)

            # Calculate the task build duration
            df_task_data['task_start_time'] = pd.to_datetime(df_task_data['task_start_time']).dt.tz_localize(None)
            df_task_data['task_end_time'] = pd.to_datetime(df_task_data['task_end_time']).dt.tz_localize(None)
            df_task_data['task_build_time'] = (df_task_data['task_end_time'] - df_task_data['task_start_time'])
        except (KeyError, TypeError, ValueError) as err:
            log_error(self.str_class, mod_str, type(err).__name__, str(err))
            return None
        except Exception as e:
            log_error(self.str_class, mod_str, 'Exception', str(e))
            return None

        # Helper function to convert timedelta to HH:MM:SS format
        def format_timedelta(td):
            try:
                if pd.isna(td):  # Check if the timedelta is NaN
                    return "00:00:00"

                total_seconds = int(td.total_seconds())
                hours = total_seconds // 3600
                minutes = (total_seconds % 3600) // 60
                seconds = total_seconds % 60
                return f"{hours:02}:{minutes:02}:{seconds:02}"
            except (KeyError, TypeError, ValueError) as err:
                log_error(self.str_class, mod_str, type(err).__name__, str(err))
                return None
            except Exception as e:
                log_error(self.str_class, mod_str, 'Exception', str(e))
                return None

        try:
            # Apply the formatting function
            df_task_data['task_build_time'] = df_task_data['task_build_time'].apply(format_timedelta)

            df_task_data['task_num'] = df_task_data['task_num'].fillna(0.0)

            df_task_data = df_task_data.sort_values(by=['task_num'], ascending=True, na_position='last', ignore_index=True)
            return df_task_data
        except (KeyError, TypeError, ValueError) as err:
            log_error(self.str_class, mod_str, type(err).__name__, str(err))
            return pd.DataFrame()
        except Exception as e:
            log_error(self.str_class, mod_str, 'Exception', str(e))
            return pd.DataFrame()


    # def process_bypasses(self, data: pd.DataFrame) -> pd.DataFrame:
    #     mod_str = 'process_bypasses'
    #     df_data = data.copy()
    #
    #     # Initialize new columns
    #     df_data['fail_reason'] = None
    #     df_data['supervisor_override'] = None
    #     df_data['route_to'] = None
    #     df_data['override_timestamp'] = None
    #     def process_fail_reason(json_str: str) -> Tuple[str, str, str, str]:
    #         try:
    #             # Load input string as json object
    #             data = json.loads(json_str)
    #
    #             # Extract fail_reason
    #             fail_reason = data.get('fail reason', '')
    #
    #             # Extract supervisor_override
    #             rework_confirmed_by = data.get('reworkConfirmedBy', {})
    #             supervisor_name = rework_confirmed_by.get('name', '')
    #             badge_id = rework_confirmed_by.get('badgeId', '')
    #             supervisor_override = f"{supervisor_name} ({badge_id})" if supervisor_name and badge_id else ''
    #
    #             # Extract route_to
    #             functions = rework_confirmed_by.get('functions', [])
    #             route_to = [func for func in functions if func not in ["supervisor", "operator"]]
    #             route_to = ', '.join(route_to) if route_to else ''
    #
    #             # Convert override_timestamp to datetime
    #             record_time = rework_confirmed_by.get('recordTime', None)
    #             override_timestamp = pd.to_datetime(record_time) if record_time else ''
    #             return fail_reason, supervisor_override, route_to, override_timestamp
    #         except (JSONDecodeError, KeyError, TypeError, ValueError, AttributeError) as err:
    #             log_error(self.str_class, mod_str, type(err).__name__, str(err))
    #             return '', '', '', ''
    #         except Exception as e:
    #             log_error(self.str_class, mod_str, 'Exception', str(e))
    #             return '', '', '', ''
    #
    #     def process_manual_override(json_str: str) -> Tuple[str, str]:
    #         try:
    #             data = json.loads(json_str)
    #
    #             # Extract supervisor_override
    #             manually_confirmed_by = data.get('manuallyCompletedBy', None)
    #             supervisor_name = manually_confirmed_by.get('name', '')
    #             badge_id = manually_confirmed_by.get('badgeId', '')
    #             supervisor_override = f"{supervisor_name} ({badge_id})" if supervisor_name and badge_id else ''
    #
    #             # Convert override timestamp to datetime
    #             record_time = manually_confirmed_by.get('recordTime', '')
    #             override_timestamp = pd.to_datetime(record_time) if record_time else ''
    #             return supervisor_override, override_timestamp
    #         except (JSONDecodeError, KeyError, TypeError, ValueError, AttributeError) as err:
    #             log_error(self.str_class, mod_str, type(err).__name__, str(err))
    #             return '', ''
    #         except Exception as e:
    #             log_error(self.str_class, mod_str, 'Exception', str(e))
    #             return '', ''
    #
    #     def format_timedelta(td: timedelta) -> str:
    #         # Helper function to convert timedelta to HH:MM:SS format
    #         try:
    #             if pd.isna(td):  # Check if the timedelta is NaN
    #                 return "00:00:00"
    #
    #             total_seconds = int(td.total_seconds())
    #             hours = total_seconds // 3600
    #             minutes = (total_seconds % 3600) // 60
    #             seconds = total_seconds % 60
    #             return f"{hours:02}:{minutes:02}:{seconds:02}"
    #         except (ValueError, TypeError, AttributeError) as val_err:
    #             logger.error(self.str_class, mod_str, type(err).__name__, str(err))
    #             return "00:00:00"
    #         except Exception as e:
    #             logger.error(self.str_class, mod_str, 'Exception', str(e))
    #             return "00:00:00"
    #
    #     # Process bypass data
    #     try:
    #         task_data = data.copy()
    #
    #         # Validate dataframe not empty
    #         if task_data.empty:
    #             raise ValueError('task_data is an empty DataFrame')
    #
    #         # Exclude failed and bypassed tasks
    #         df_task_data = (task_data[(task_data['task_status'] != 'FAILED') &
    #                                   (task_data['task_status'] != 'COMPLETED (MANUALLY)')]
    #                         .reset_index())
    #         df_task_data.drop(columns=['extra_data_json', 'index'], inplace=True)
    #         df_task_data = df_task_data.fillna('')
    #         # df_task_data['override_timestamp'] = pd.to_datetime(df_task_data['override_timestamp'], utc=True)
    #
    #         # If failed tasks define bypass override input values
    #         df_failed_tasks = task_data[task_data['task_status'] == 'FAILED']
    #         if df_failed_tasks is not None and not df_failed_tasks.empty:
    #             (df_failed_tasks['fail_reason'], df_failed_tasks['supervisor_override'],
    #              df_failed_tasks['route_to'], df_failed_tasks['override_timestamp']) = zip(*df_failed_tasks['extra_data_json']
    #                                                                                        .apply(process_fail_reason))
    #             df_failed_tasks = df_failed_tasks.drop(columns=['extra_data_json']).fillna('')
    #             df_task_data = pd.concat([df_task_data, df_failed_tasks], ignore_index=True)
    #
    #         # Apply function to parse manual bypass input
    #         df_bypassed_tasks = task_data[task_data['task_status'] == 'COMPLETED (MANUALLY)']
    #         if df_bypassed_tasks is not None and not df_bypassed_tasks.empty:
    #             (df_bypassed_tasks['supervisor_override'],
    #              df_bypassed_tasks['override_timestamp']) = zip(*df_bypassed_tasks['extra_data_json']
    #                                                             .apply(process_manual_override))
    #             df_bypassed_tasks = df_bypassed_tasks.drop(columns=['extra_data_json']).fillna('')
    #             df_task_data = pd.concat([df_task_data, df_bypassed_tasks], ignore_index=True)
    #
    #         # Calculate the task build duration
    #         df_task_data['task_start_time'] = pd.to_datetime(df_task_data['task_start_time']).dt.tz_localize(None)
    #         df_task_data['task_end_time'] = pd.to_datetime(df_task_data['task_end_time']).dt.tz_localize(None)
    #         df_task_data['task_build_time'] = (df_task_data['task_end_time'] - df_task_data['task_start_time'])
    #
    #         # Apply the formatting function
    #         df_task_data['task_build_time'] = df_task_data['task_build_time'].apply(format_timedelta)
    #
    #         df_task_data['task_num'] = pd.to_numeric(df_task_data['task_num'], errors='coerce')
    #         df_task_data['task_num'] = (df_task_data['task_num']
    #                                     .infer_objects(copy=False)
    #                                     .fillna(0.0))
    #
    #         df_task_data = (df_task_data
    #                         .sort_values(by=['task_num'],
    #                                      ascending=True,
    #                                      na_position='last',
    #                                      ignore_index=True))
    #         return df_task_data
    #     except (KeyError, TypeError, AttributeError, ValueError) as err:
    #         log_error(self.str_class, mod_str, type(err).__name__, str(err))
    #         return df_data
    #     except Exception as e:
    #         log_error(self.str_class, mod_str, 'Exception', str(e))
    #         return df_data
