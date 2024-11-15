from dataclasses import dataclass, field
import os
from typing import NamedTuple


import pandas as pd


from config.config import INPUT_FILENAME, INPUT_FOLDER, OUTPUT_FOLDER


@dataclass(kw_only=True)
class PandasDataFrameRow:
    """
    A class for parsing Pandas dataframe rows with automatic file name generation.
    """
    row: NamedTuple
    _row_df: pd.DataFrame = field(init=False, default=None)


    # Output path args
    _output_path: str = field(init=False, default=None)
    _output_path_args: str = field(init=False, default=None)
    _output_file_name_postfix: str = field(init=False, default="")
    _output_file_name: str = field(init=False, default=None)
    _output_file_end: str = field(init=False, default=".csv")


    # Input path args
    _input_path: str = field(init=False, default=None)
    _input_path_args: str = field(init=False, default=None)
    _input_file_name: str = field(init=False, default=INPUT_FILENAME)


    def __post_init__(self):
        # Initialize tracked attributes
        self._tracked_attrs = set()
        self._needs_update = True


    def clean_file_name(self, *args) -> str:
        return "_".join(str(arg) for arg in args if arg)


    def make_file_name(self, *args) -> str:
        output_name = [str(arg).replace(" ", "_").lower() for arg in args if arg]
        output_name = "_".join(output_name)
        return self.clean_file_name(output_name)


    def make_path(self, *args) -> str:
        return os.path.join(*[str(arg) for arg in args if arg])


    def make_output_path(self, *args) -> str:
        return self.make_path(OUTPUT_FOLDER, *args)


    def make_input_path(self, *args) -> str:
        return self.make_path(INPUT_FOLDER, *args)


    @property
    def output_file_name_postfix(self) -> str:
        return self._output_file_name_postfix


    @output_file_name_postfix.setter
    def output_file_name_postfix(self, value: str):
        self._output_file_name_postfix = value
        self._needs_update = True


    @property
    def output_file_name(self) -> str:
        if self._needs_update:
            self._regenerate_output_file_name()
        return self._output_file_name


    @output_file_name.setter
    def output_file_name(self, value: str):
        self._output_file_name = value


    @property
    def row_df(self) -> str:
        return self._row_df


    @row_df.setter
    def row_df(self, value: pd.DataFrame):
        self._row_df = value


    @property
    def output_file_end(self) -> str:
        return self._output_file_end


    @row_df.setter
    def output_file_end(self, value: str):
        self._output_file_end = value


    def _regenerate_output_file_name(self):
        """Regenerates the output file name based on current attribute values."""
        self._needs_update = False
        self._output_file_name = self.make_file_name(
            getattr(self, '_current_base_name', ''),
            self.output_file_name_postfix,
            self.output_file_end
        )


    def __setattr__(self, name, value):
        """
        Custom attribute setter that tracks changes to relevant attributes
        and triggers file name updates when needed.
        """
        super().__setattr__(name, value)
        
        # Check if this is an attribute we should track
        if name in self._tracked_attrs:
            self._needs_update = True
            self._regenerate_output_file_name()


@dataclass(kw_only=True)
class MuniRow(PandasDataFrameRow):
    """
    Represents a row object from a Municode library CSV file.
    """
    def __post_init__(self):
        super().__post_init__()


        # Initialize tracked attributes
        self._tracked_attrs = {'place_name', 'gnis', 'output_file_name_postfix', 'output_file_end'}


        # Set initial values from row
        self.url = self.row.url
        self.place_name = self.row.place_name
        self.gnis = self.row.gnis


        # Set base name for file generation
        self._current_base_name = f"{self.place_name}_{self.gnis}"


        # Initial generation of output file name
        self._regenerate_output_file_name()


        # Set paths
        self.input_path = self.make_input_path(self.output_file_name)
        self.output_path = self.make_output_path(self.output_file_name)


    def __setattr__(self, name, value):
        super().__setattr__(name, value)
        
        # Update base name if relevant attributes change
        if name in {'place_name', 'gnis'}:
            self._current_base_name = f"{self.place_name}_{self.gnis}"
            self._regenerate_output_file_name()