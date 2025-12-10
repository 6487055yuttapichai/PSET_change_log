import pandas as pd
import numpy as np
import panel as pn
from panel.pane import Plotly, Matplotlib
import plotly.express as px
import plotly.express as px
import plotly.graph_objs as go
from plotly.graph_objs import Figure
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import norm

from shared.tdm_logging import logger, log_error, class_method_name


class PlotTraceCurves:
    def __init__(self):
        pass

    @staticmethod
    def torque_vs_time(fig: Plotly, trace_data: pd.DataFrame) -> go.Figure | Plotly:
        """
        Generates a torque vs. time plot using data from the provided DataFrame.

        This method creates a Plotly Figure and adds traces to it, where each
        trace represents torque over time data for a specific device and tightening ID.
        The plot is configured with titles, labels, and hover information for better
        readability and interaction.

        Parameters:
            fig (go.Figure): A Plotly Figure for creating a Plotly Line chart
            trace_data (pd.DataFrame): A DataFrame containing the torque and time data
                                       along with additional necessary information like
                                       device, tightening ID, and units.

        Returns:
            go.Figure: The Plotly Figure object with the torque vs. time traces added.

        Raises:
            KeyError: If a required column is missing in the DataFrame.
            TypeError: If there are data type inconsistencies in the DataFrame.
        """
        df_trace = trace_data.copy()
        # expected_columns = ['device', 'tightening_id', 'time_sample', 'torque_sample', 'time_unit', 'torque_unit']

        if df_trace is None:
            print("Invalid or empty DataFrame.")
            return fig

        df_trace.rename(columns={'time_sample': 'Time (ms)', 'torque_sample': 'Torque (Nm)'}, inplace=True)
        fig = px.line(df_trace, x='Time (ms)', y='Torque (Nm)', title='Torque vs. Time', color='Rundown')
        # fig.update_layout(xaxis_title='Time (ms)', yaxis_title='Torque (Nm)')
        return fig

    @staticmethod
    def angle_vs_time(fig: Plotly, trace_data: pd.DataFrame) -> go.Figure | Plotly:
        """
        Generates a angle vs. time plot using data from the provided DataFrame.

        This method creates a Plotly Figure and adds traces to it, where each
        trace represents angle over time data for a specific device and tightening ID.
        The plot is configured with titles, labels, and hover information for better
        readability and interaction.

        Parameters:
            fig (go.Figure): A Plotly Figure for creating a Plotly Line chart
            trace_data (pd.DataFrame): A DataFrame containing the angle and time data
                                       along with additional necessary information like
                                       device, tightening ID, and units.

        Returns:
            go.Figure: The Plotly Figure object with the torque vs. time traces added.

        Raises:
            KeyError: If a required column is missing in the DataFrame.
            TypeError: If there are data type inconsistencies in the DataFrame.
        """
        df_trace = trace_data.copy()
        # expected_columns = ['device', 'tightening_id', 'time_sample', 'angle_sample', 'time_unit', 'angle_unit']

        if df_trace is None:
            print("Invalid or empty DataFrame.")
            return fig

        df_trace.rename(columns={'time_sample': 'Time (ms)', 'angle_sample': 'Angle (deg)'}, inplace=True)
        fig = px.line(df_trace, x='Time (ms)', y='Angle (deg)', title='Angle vs. Time', color='Rundown')
        # fig.update_layout(xaxis_title='Time (ms)', yaxis_title='Angle (deg)')
        return fig

    @staticmethod
    def torque_vs_angle(fig: Plotly, trace_data: pd.DataFrame) -> go.Figure | Plotly:
        """
        Generates a torque vs. angle plot using data from the provided DataFrame.

        This method creates a Plotly Figure and adds traces to it, where each
        trace represents torque vs angle data for a specific device and tightening ID.
        The plot is configured with titles, labels, and hover information for better
        readability and interaction.

        Parameters:
            fig (go.Figure): A Plotly Figure for creating a Plotly Line chart
            trace_data (pd.DataFrame): A DataFrame containing the torque and angle data
                                       along with additional necessary information like
                                       device, tightening ID, and units.

        Returns:
            go.Figure: The Plotly Figure object with the torque vs. time traces added.

        Raises:
            KeyError: If a required column is missing in the DataFrame.
            TypeError: If there are data type inconsistencies in the DataFrame.
        """
        df_trace = trace_data.copy()
        # expected_columns = ['device', 'tightening_id', 'angle_sample', 'torque_sample', 'angle_unit', 'torque_unit']

        if df_trace is None:
            print("Invalid or empty DataFrame.")
            return fig

        df_trace.rename(columns={'torque_sample': 'Torque (Nm)', 'angle_sample': 'Angle (deg)'}, inplace=True)
        fig = px.line(df_trace, x='Angle (deg)', y='Torque (Nm)', title='Torque vs. Angle', color='Rundown')
        # fig.update_layout(xaxis_title='Angle (deg)', yaxis_title='Torque (Nm)')
        return fig

    @staticmethod
    def current_vs_angle(fig: Plotly, trace_data: pd.DataFrame) -> go.Figure | Plotly:
        """
        Generates a current vs. angle plot using data from the provided DataFrame.

        This method creates a Plotly Figure and adds traces to it, where each
        trace represents torque vs angle data for a specific device and tightening ID.
        The plot is configured with titles, labels, and hover information for better
        readability and interaction.

        Parameters:
            fig (go.Figure): A Plotly Figure for creating a Plotly Line chart
            trace_data (pd.DataFrame): A DataFrame containing the torque and angle data
                                       along with additional necessary information like
                                       device, tightening ID, and units.

        Returns:
            go.Figure: The Plotly Figure object with the torque vs. time traces added.

        Raises:
            KeyError: If a required column is missing in the DataFrame.
            TypeError: If there are data type inconsistencies in the DataFrame.
        """
        df_trace = trace_data.copy()
        # expected_columns = ['device', 'tightening_id', 'angle_sample', 'torque_sample', 'angle_unit', 'torque_unit']

        if df_trace is None:
            print("Invalid or empty DataFrame.")
            return fig

        df_trace.rename(columns={'current_sample': 'Current (mA)', 'angle_sample': 'Angle (deg)'}, inplace=True)
        fig = px.line(df_trace, x='Angle (deg)', y='Current (mA)', title='Current vs. Angle', color='Rundown')
        # fig.update_layout(xaxis_title='Angle (deg)', yaxis_title='Torque (Nm)')
        return fig

    @staticmethod
    def current_vs_torq_vs_angle(fig: Plotly, trace_data: pd.DataFrame) -> go.Figure | Plotly:
        """
        Generates an overlayed line chart with current vs. angle plot and torque vs angle plot using data from the provided DataFrame.

        This method creates a Plotly Figure and adds traces to it, where each
        trace represents torque vs angle data for a specific device and tightening ID.
        The plot is configured with titles, labels, and hover information for better
        readability and interaction.

        Parameters:
            fig (go.Figure): A Plotly Figure for creating a Plotly Line chart
            trace_data (pd.DataFrame): A DataFrame containing the torque and angle data
                                       along with additional necessary information like
                                       device, tightening ID, and units.

        Returns:
            go.Figure: The Plotly Figure object with the torque vs. time traces added.

        Raises:
            KeyError: If a required column is missing in the DataFrame.
            TypeError: If there are data type inconsistencies in the DataFrame.
        """
        df_trace = trace_data.copy()

        # Validate not empty data
        if df_trace is None:
            print("Invalid or empty DataFrame.")
            return fig

        # Rename columns
        df_trace.rename(columns={'current_sample': 'Current Trace', 'torque_sample': 'Torque Trace'}, inplace=True)

        # Melt the DataFrame to plot multiple lines with plotly express
        df_melted = df_trace.melt(id_vars='angle_sample', value_vars=['Current Trace', 'Torque Trace'],
                                  var_name='Trace Curve', value_name='trace_sample')

        # Create the line plot using plotly express
        fig = px.line(df_melted, x='angle_sample', y='trace_sample', color='Trace Curve',
                      labels={'trace_sample': 'value', 'angle_sample': 'Angle (deg)'},
                      title="Current / Torque vs Angle")
        return fig


class PlotCpk:
    def __init__(self):
        pass

    @staticmethod
    def cpk_chart_plotly(sample_results: dict):
        class_method = class_method_name()
        default_plot = px.histogram(nbins=10, marginal="box", title="Process Capability Analysis")
        try:
            # Sample data definitions
            lsl = sample_results['lsl']
            usl = sample_results['usl']
            mean = sample_results['mean']
            std = sample_results['std']
            data = sample_results['sample']
            
            # Create histogram with Plotly
            fig = px.histogram(data, nbins=20, marginal="box", title="Process Capability Analysis")
            fig.add_vline(x=lsl, line_dash="dash", line_color="red", annotation_text="LSL")
            fig.add_vline(x=usl, line_dash="dash", line_color="orange", annotation_text="USL")
            fig.add_vline(x=mean, line_dash="dash", line_color="green", annotation_text=f"Mean = {mean:.2f}")

            return Plotly(fig)
        except (IndexError, TypeError, ValueError, AttributeError) as err:
            log_error(class_method, type(err).__name__, str(err))
            return default_plot
        except Exception as ex:
            log_error(class_method, 'Exception', str(ex))
            return default_plot

    @staticmethod
    def cpk_chart_matplot(sample_result: dict):
        class_method = class_method_name()
        plt.figure(figsize=(12, 4))
        try:
            # Sample data definitions
            lsl = sample_result['lsl']
            usl = sample_result['usl']
            mean = sample_result['mean']
            std = sample_result['std']
            data = sample_result['sample']
            tool = sample_result['tool']
            
            # Create histogram with density plot
            # plt.figure(figsize=(12, 4))
            # plt.figure()
            sns.histplot(data, bins=20, kde=True, color='lightgrey', edgecolor='black', stat='density')
            
            # Overlay the normal distribution
            x = np.linspace(min(data), max(data), 1000)
            y = norm.pdf(x, mean, std)
            plt.plot(x, y, color='blue', linestyle='--', label='Normal Distribution')
            
            # Add specification limits
            plt.axvline(lsl, color='red', linestyle='--', label='LSL')
            plt.axvline(usl, color='orange', linestyle='--', label='USL')
            plt.axvline(mean, color='green', linestyle='-', label='Mean')
            
            # Title and labels
            # plt.title('Process Capability Analysis')
            plt.title(f"""Capability Histogram""")
            plt.xlabel('Measurement')
            plt.ylabel('Density')
            plt.legend()
            # plt.gcf()
            
            # Create a Matplotlib pane for the figure
            return plt.gcf()
        except (IndexError, TypeError, ValueError, AttributeError) as err:
            log_error(class_method, type(err).__name__, str(err))
            return plt
        except Exception as ex:
            log_error(class_method, 'Exception', str(ex))
            return plt

    @staticmethod
    def spc_x_chart(control_data: pd.DataFrame, sample_results: dict):
        class_method = class_method_name()
        try:
            if control_data.empty:
                logger.info(f"control_data is an empty DataFrame.")
                return plt.figure()

            # fig = plt.figure()
            # ax = fig.add_subplot(111)
            fig, ax = plt.subplots(figsize=(12, 3))

            sns.lineplot(x='subgroup_id', y='mean', data=control_data, marker='o', ax=ax)
            ax.axhline(sample_results['xbar_bar'], color='green', linestyle='--', label='X̄̄')
            ax.axhline(sample_results['xbar_ucl'], color='red', linestyle='--', label='UCL')
            ax.axhline(sample_results['xbar_lcl'], color='red', linestyle='--', label='LCL')
            ax.set_title('X̄ (Mean) Control Chart')
            ax.set_xlabel('Subgroup ID')
            ax.set_ylabel('Subgroup Mean')
            ax.legend()
            ax.grid(True)
            fig.tight_layout()

            # Plot X̄ Chart
            # plt.figure(figsize=(12, 4))
            # plt.figure()
            # sns.lineplot(x='subgroup_id', y='mean', data=control_data, marker='o')
            # plt.axhline(sample_results['xbar_bar'], color='green', linestyle='--', label='X̄̄')
            # plt.axhline(sample_results['xbar_ucl'], color='red', linestyle='--', label='UCL')
            # plt.axhline(sample_results['xbar_lcl'], color='red', linestyle='--', label='LCL')
            # plt.title('X̄ (Mean) Control Chart')
            # plt.xlabel('Subgroup ID')
            # plt.ylabel('Subgroup Mean')
            # plt.legend()
            # plt.grid(True)
            # plt.tight_layout()
            return fig
        except (IndexError, TypeError, ValueError, AttributeError) as err:
            log_error(class_method, type(err).__name__, str(err))
            return None
        except Exception as ex:
            log_error(class_method, 'Exception', str(ex))
            return None

    @staticmethod
    def spc_r_chart(control_data: pd.DataFrame, sample_results: dict):
        class_method = class_method_name()
        plt.figure(figsize=(12, 3))
        # plt.figure()
        try:
            if control_data.empty:
                logger.info(f"control_data is an empty DataFrame.")
                return plt

            # Plot R Chart
            # plt.figure(figsize=(12, 4))
            # plt.figure()
            sns.lineplot(x='subgroup_id', y='range', data=control_data, marker='o')
            plt.axhline(sample_results['r_bar'], color='green', linestyle='--', label='R̄')
            plt.axhline(sample_results['r_ucl'], color='red', linestyle='--', label='UCL')
            plt.axhline(sample_results['r_lcl'], color='red', linestyle='--', label='LCL')
            plt.title('R (Range) Control Chart')
            plt.xlabel('Subgroup ID')
            plt.ylabel('Subgroup Range')
            plt.legend()
            plt.grid(True)
            plt.tight_layout()
            return plt.gcf()
        except (IndexError, TypeError, ValueError, AttributeError) as err:
            log_error(class_method, type(err).__name__, str(err))
            return plt
        except Exception as ex:
            log_error(class_method, 'Exception', str(ex))
            return plt

    @staticmethod
    def spc_value_chart(control_data: pd.DataFrame, sample_results: dict):
        class_method = class_method_name()
        """
        Plots a scatter plot of all tool's PSET sample values with LSL, USL, and mean lines.

        Parameters:
        - control_data: a DataFrame with a 'sample_values' column (each row is a list or set of values)
        - sample_results: dictionary with 'lsl', 'usl', and 'mean' values
        """
        fig, ax = plt.subplots(figsize=(12, 3))
        # fig, ax = plt.subplots()
        try:
            if control_data.empty:
                logger.info(f"control_data is an empty DataFrame.")
                return plt

            # Flatten all sample_values into a single list
            all_values = []
            for row in control_data['sample_values']:
                # Convert set to list if needed
                if isinstance(row, set):
                    row = list(row)
                all_values.extend(row)

            # Create scatter plot
            # plt.figure(figsize=(12, 4))
            ax.scatter(range(len(all_values)), all_values, alpha=0.7, label='Sample Values')

            # Draw LSL, USL, and Mean lines
            # lsl = sample_results['lsl']
            # usl = sample_results['usl']
            # mean = sample_results['mean']

            # ax.axhline(y=lsl, color='red', linestyle='--', label=f'LSL ({lsl})')
            # ax.axhline(y=usl, color='red', linestyle='--', label=f'USL ({usl})')
            # plt.axhline(y=mean, color='green', linestyle='-', label=f'Mean ({mean})')

            # Title and labels
            ax.set_title(f"Last 25 Subgroups")
            ax.set_xlabel("Sample Index")
            ax.set_ylabel("Torque Value")
            ax.legend(loc='upper right')
            ax.grid(True)
            fig.tight_layout()
            return fig
        except (IndexError, TypeError, ValueError, AttributeError) as err:
            log_error(class_method, type(err).__name__, str(err))
            return None
        except Exception as ex:
            log_error(class_method, 'Exception', str(ex))
            return None