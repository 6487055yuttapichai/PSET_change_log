import json
from datetime import datetime, timedelta
import pandas as pd
from pandas import DataFrame
from pandas.errors import EmptyDataError

from .tdm_logging import logger, log_error, class_method_name


class TraceCurves:
    """
    A class to process and manage trace curves data from tightening devices, primarily focusing on
    torque, angle, and time samples for different types of traces such as torque trace, angle trace,
    and desoutter trace.

    Version:
    ----------
    version: v1.1
    date: 25 May, 2024
    change:

    Attributes:
    ----------
    df_traces : pd.DataFrame
        A DataFrame that stores the processed trace data, including torque, angle, and time samples.
    torque_unit : str
        The unit for torque data (default: 'Nm').
    angle_unit : str
        The unit for angle data (default: 'deg').
    time_unit : str
        The unit for time data (default: 'ms').
    """
    df_traces = pd.DataFrame()
    torque_unit = 'Nm'
    angle_unit = 'deg'
    time_unit = 'ms'

    def __init__(self, trace_data: pd.DataFrame):
        """
        Initializes the TraceCurves object with raw trace data and processes it.

        Parameters:
        -----------
        trace_data : pd.DataFrame
            The raw trace data containing device and tightening information.
        """
        self.df_traces = self.process_trace_curves(trace_data)

    def get_trace_data(self) -> tuple[DataFrame, DataFrame, DataFrame]:
        """
        Returns the processed torque and angle trace data.

        Returns:
        --------
        tuple[pd.DataFrame, pd.DataFrame]
            A tuple containing two DataFrames:
            - df_torq_v_time: DataFrame with torque data versus time.
            - df_angle_v_time: DataFrame with angle data versus time.
            - df_curr_v_angle: DataFrame with current data versus angle.
        """
        return self.df_traces

    def process_trace_curves(self, trace_data: pd.DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
        """
        Processes the trace data to extract and organize torque and angle traces

        Parameters:
        -----------
        trace_data : pd.DataFrame
            The input DataFrame containing trace data, including device_id, tightening_id,
            and trace information in JSON format.

        Returns:
        --------
        tuple[pd.DataFrame, pd.DataFrame]
            A tuple containing two DataFrames:
            - df_torq_v_time: DataFrame with torque versus time samples.
            - df_angle_v_time: DataFrame with angle versus time samples.
        """
        class_method = class_method_name()
        logger.info(f"| {class_method} | Executing")
        start_time = datetime.now()
        try:
            # Validate trace_data is not empty dataframe
            if trace_data.empty:
                raise EmptyDataError('trace_data is an empty DataFrame')

            df_traces = trace_data.copy()

            # Define the trace type: ['torque trace', 'angle trace', 'current trace', 'desoutter']
            df_traces['trace_type'] = df_traces.apply(self.get_trace_type, axis=1)

            # Initialize chart dataframes for plotting
            torq_cols = ['device_id', 'tightening_id', 'torque_sample', 'time_sample']
            angle_cols = ['device_id', 'tightening_id', 'angle_sample', 'time_sample']
            curr_cols = ['device_id', 'tightening_id', 'current_sample', 'time_sample']
            df_torq_v_time = pd.DataFrame(columns=torq_cols)
            df_angle_v_time = pd.DataFrame(columns=angle_cols)
            df_curr_v_angle = pd.DataFrame(columns=curr_cols)

            # Iterrate through the traces processing each curve type
            for _, row in df_traces.iterrows():
                torq_samples = pd.DataFrame(columns=torq_cols)
                angle_samples = pd.DataFrame(columns=angle_cols)
                current_samples = pd.DataFrame(columns=curr_cols)
                trace_type = row['trace_type']
                trace_data = json.loads(row['trace_data'])

                if trace_type == 'torque trace':
                    torq = self.mid_900_trace_sample(trace_data, 1.0)
                    time = self.mid_900_time_sample(trace_data)
                    torq_samples = pd.DataFrame({
                        'device_id': [row['device_id']] * len(torq),
                        'tightening_id': [row['tightening_id']] * len(torq),
                        'torque_sample': torq,
                        'time_sample': time})
                elif trace_type == 'angle trace':
                    angle = self.mid_900_trace_sample(trace_data, 1.0)
                    time = self.mid_900_time_sample(trace_data)
                    angle_samples = pd.DataFrame({
                        'device_id': [row['device_id']] * len(angle),
                        'tightening_id': [row['tightening_id']] * len(angle),
                        'angle_sample': angle,
                        'time_sample': time})
                elif trace_type == 'current trace':
                    current = self.mid_900_trace_sample(trace_data, 0.1)
                    time = self.mid_900_time_sample(trace_data)
                    current_samples = pd.DataFrame({
                        'device_id': [row['device_id']] * len(current),
                        'tightening_id': [row['tightening_id']] * len(current),
                        'current_sample': current,
                        'time_sample': time})
                elif trace_type == 'desoutter':
                    torq = self.mid_7410_trace_sample(trace_data, 'torque')
                    angle = self.mid_7410_trace_sample(trace_data, 'angle')
                    time = self.mid_7410_trace_sample(trace_data, 'time')
                    torq_samples = pd.DataFrame({
                        'device_id': [row['device_id']] * len(torq),
                        'tightening_id': [row['tightening_id']] * len(torq),
                        'torque_sample': torq,
                        'time_sample': time})
                    angle_samples = pd.DataFrame({
                        'device_id': [row['device_id']] * len(angle),
                        'tightening_id': [row['tightening_id']] * len(angle),
                        'angle_sample': angle,
                        'time_sample': time})

                # Populate torque curve samples
                if not torq_samples.empty and not df_torq_v_time.empty:
                    df_torq_v_time = pd.concat([df_torq_v_time, torq_samples], ignore_index=True)
                elif not torq_samples.empty and df_torq_v_time.empty:
                    df_torq_v_time = torq_samples

                # Populate angle curve samples
                if not angle_samples.empty and not df_angle_v_time.empty:
                    df_angle_v_time = pd.concat([df_angle_v_time, angle_samples], ignore_index=True)
                elif not angle_samples.empty and df_angle_v_time.empty:
                    df_angle_v_time = angle_samples

                # Populate current curve samples
                if not current_samples.empty and not df_curr_v_angle.empty:
                    df_curr_v_angle = pd.concat([df_curr_v_angle, current_samples], ignore_index=True)
                elif not current_samples.empty and df_curr_v_angle.empty:
                    df_curr_v_angle = current_samples

            # Combine angle sample with current sample
            if not df_curr_v_angle.empty and not df_angle_v_time.empty:
                df_curr_v_angle = df_curr_v_angle.merge(df_angle_v_time,
                                                        on=['device_id', 'tightening_id', 'time_sample'],
                                                        how='inner')
            # Combine torque sample with current sample
            if not df_curr_v_angle.empty and not df_torq_v_time.empty:
                df_curr_v_angle = df_curr_v_angle.merge(df_torq_v_time,
                                                        on=['device_id', 'tightening_id', 'time_sample'],
                                                        how='inner')
            # Log status
            end_time = datetime.now()
            logger.info(f"| {class_method} | Executed: {end_time - start_time}")
            return df_torq_v_time, df_angle_v_time, df_curr_v_angle
        except (EmptyDataError, ValueError, TypeError, IndexError, AttributeError) as err:
            log_error(class_method, type(err).__name__, str(err))
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        except Exception as ex:
            log_error(class_method, 'Exception', str(ex))
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    @staticmethod
    def get_trace_type(row) -> str:
        """
        Determines the type of trace (e.g., torque, angle, or desoutter) from a row of trace data.

        Parameters:
        -----------
        row : pd.Series
            A row of the DataFrame that contains the trace_data column in JSON format.

        Returns:
        --------
        str
            The trace type, either 'torque trace', 'angle trace', or 'desoutter'.
            Defaults to 'desoutter' if the trace type cannot be determined.
        """
        try:
            trace = json.loads(row['trace_data'])
            result = trace['payload'].get('traceType', 'desoutter').lower()
            logger.trace(str(result))
            return result
        except (json.JSONDecodeError, KeyError):
            return 'desoutter'

    @staticmethod
    def mid_900_trace_sample(trace_data: json, coefficient: float) -> list[float]:
        """
        Processes trace samples for 'mid_900' devices by applying a scaling coefficient.

        Parameters:
        -----------
        trace_data : json
            JSON-formatted trace data containing the trace samples.
        coefficient : float
            A scaling factor for the trace samples.

        Returns:
        --------
        list[float]
            A list of scaled torque or angle samples.
        """
        try:
            trace_sample = trace_data['payload']['traceSample']
            return [(abs(x) * coefficient) for x in trace_sample]
        except (json.JSONDecodeError, KeyError):
            return []

    @staticmethod
    def mid_900_time_sample(trace_data: json) -> list[float]:
        """
        Processes time samples for 'mid_900' devices based on resolution fields.

        Parameters:
        -----------
        trace_data : json
            JSON-formatted trace data containing resolution field and index data.

        Returns:
        --------
        list[float]
            A list of time values for the trace samples, calculated based on index ranges.
        """
        try:
            fields = trace_data['payload']['resolutionFields'][0]
            first_index = fields['firstIndex']
            last_index = fields['lastIndex']
            time_value = float(fields['timeValue'])
            return [round(i * time_value, 2) for i in range(first_index, last_index)]
        except (json.JSONDecodeError, KeyError):
            return []

    @staticmethod
    def mid_7410_trace_sample(trace_data: json, trace_type: str) -> list[float]:
        """
        Processes torque, angle, or time samples for 'mid_7410' devices based on the trace type.

        Parameters:
        -----------
        trace_data : json
            JSON-formatted trace data containing the curve data and coefficients.
        trace_type : str
            The type of sample to extract: 'torque', 'angle', or 'time'.

        Returns:
        --------
        list[float]
            A list of torque, angle, or time samples depending on the trace_type.
        """
        try:
            curve_data = trace_data['payload']['curveData']
            if trace_type == 'torque':
                coefficient = trace_data['payload']['torqueCoefficient']
                return [x[0] * coefficient for x in curve_data]
            elif trace_type == 'angle':
                coefficient = trace_data['payload']['angleCoefficient']
                return [x[1] * coefficient for x in curve_data]
            elif trace_type == 'time':
                coefficient = trace_data['payload']['timeCoefficient']
                return [(i * coefficient) * 100 for i in range(len(curve_data))]
            else:
                return []
        except (json.JSONDecodeError, KeyError):
            return []
