import numpy as np
import pandas as pd



class Analyzer ():
    def __init__(self, dataframe: pd.DataFrame):
        self.dataframe = dataframe.copy()


    def _apply_filters(self, dataframe: pd.DataFrame, filters: dict = None) -> pd.DataFrame:
        if not filters:
            return dataframe
            
        filtered_dataframe = dataframe.copy()

        for col, value in filters.items():
            if col in filtered_dataframe.columns:
                if isinstance(value, list):
                    filtered_dataframe = filtered_dataframe[filtered_dataframe[col].isin(value)]
                
                else:
                    filtered_dataframe = filtered_dataframe[filtered_dataframe[col] == value]
                    
        return filtered_dataframe


    def _apply_computed_rules(self, dataframe: pd.DataFrame, rules: list) -> pd.DataFrame:
        if not rules:
            return dataframe
            
        result_dataframe = dataframe.copy()
        
        for rule in rules:
            
            if "where" in rule:
                mask = result_dataframe.eval(rule["where"])
                result_dataframe.loc[mask, rule["set_col"]] = rule["set_value"]

            elif "expression" in rule:
                expr = f"{rule['new_col']} = {rule['expression']}"
                result_dataframe.eval(expr, inplace = True)

            elif "method" in rule:
                src = rule["source_col"]
                method_name = rule["method"]
                
                result_dataframe[rule["new_col"]] = getattr(result_dataframe[src], method_name)()
                
        return result_dataframe

    
    def get_categorical_distribution(
            self,
            x_col: str,
            top_n: int | None = None,
            filters: dict | None = None,
            rules: list | None = None,
            orient: str = 'records',
            **kwargs
        ) -> list:
        data = self._apply_filters(self.dataframe, filters)
        
        if data.empty or x_col not in data.columns: return []

        grouped = data[x_col].value_counts().reset_index()
        grouped.columns = ['x', 'y']
        
        if top_n:
            grouped = grouped.head(top_n)
            
        grouped = self._apply_computed_rules(grouped, rules)

        if kwargs:
            for key, value in kwargs.items():
                grouped[key] = value
                
        return grouped.to_dict(orient = orient)


    def get_numerical_aggregation(
            self,
            x_col: str,
            y_col: str,
            agg_func: str = 'mean',
            top_n: int | None = None,
            filters: dict | None = None,
            rules: list | None = None,
            orient: str = 'records',
            **kwargs
        ) -> list:
        data = self._apply_filters(self.dataframe, filters)
        if data.empty or x_col not in data.columns or y_col not in data.columns: return []

        valid_data = data.dropna(subset=[y_col])
        
        grouped = valid_data.groupby(x_col)[y_col].agg(agg_func).round(0).reset_index()
        grouped.columns = ['x', 'y']
        grouped = grouped.sort_values(by = 'y', ascending=False)
        
        if top_n:
            grouped = grouped.head(top_n)
            
        grouped = self._apply_computed_rules(grouped, rules)

        if kwargs:
            for key, value in kwargs.items():
                grouped[key] = value
                
        return grouped.to_dict(orient = orient)


    def get_time_series(
            self,
            time_period: str = 'M',
            num_col: str = None,
            agg_func: str = 'count',
            filters: dict | None = None,
            rules: list | None = None,
            orient: str = 'records',
            **kwargs
        ) -> list:
        data = self._apply_filters(self.dataframe, filters)
        if data.empty or 'created' not in data.columns: return []

        data = data.dropna(subset=['created']).copy()
        
        if time_period.upper() == 'M':
            data['time_x'] = data['created'].dt.strftime('%Y-%m')
        
        elif time_period.upper() == 'Y':
            data['time_x'] = data['created'].dt.strftime('%Y')
        
        else:
            data['time_x'] = data['created'].dt.strftime('%Y-%m-%d')


        if num_col and agg_func != 'count':
            grouped = data.groupby('time_x')[num_col].agg(agg_func).round(0).reset_index()
        
        else:
            grouped = data.groupby('time_x').size().reset_index(name = 'y')

        grouped.columns = ['x', 'y']
        grouped = grouped.sort_values(by='x', ascending=True)
       
        grouped = self._apply_computed_rules(grouped, rules)

        if kwargs:
            for key, value in kwargs.items():
                grouped[key] = value
                
        return grouped.to_dict(orient = orient)


    def get_kpi_value(self, column: str, agg_func: str = 'count', filters: dict | None = None, **kwargs) -> dict:
        data = self._apply_filters(self.dataframe, filters)
        
        if agg_func == 'count':
            value = len(data)
        else:
            value = data[column].agg(agg_func)
            
        kpi_card = {
            "value": round(value, 2) if isinstance(value, float) else value,
            "label": kwargs.get("label", column)
        }

        if kwargs:
            for key, val in kwargs.items():
                if key != "label":
                    kpi_card[key] = val
                    
        return kpi_card

    
    def get_bins(self, col: str, bins: int = 10, filters: dict = None, rules: list = None, orient: str = 'records', **kwargs) -> list:
        data = self._apply_filters(self.dataframe, filters)
        
        if data.empty or col not in data.columns: return []
        
        counts, bin_edges = np.histogram(data[col].dropna(), bins = bins)
        
        bin_labels = [f"{int(bin_edges[i])}-{int(bin_edges[i + 1])}" for i in range(len(bin_edges) - 1)]
        grouped = pd.DataFrame({'x': bin_labels, 'y': counts})
        
        grouped = self._apply_computed_rules(grouped, rules)
       
        if kwargs:
            for key, value in kwargs.items(): grouped[key] = value
            
        return grouped.to_dict(orient = orient)