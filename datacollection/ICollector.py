"Module with abstract base class ICollector, the interface for data collectors used in this project."
from abc import ABC, abstractmethod
import pandas as pd
import argparse
import datetime
from tqdm import tqdm


class ICollector(ABC):
    r"""
    Interface for data collectors used in this project.
    """

    def __init__(self, sourcelabel: str) -> None:
        r"""
        Initializes the base class
        """
        self._sourcelabel: str = sourcelabel
        self._parser: argparse.ArgumentParser = self._setupparser()

    def __call__(self) -> None:
        r"""
        Calls the collection process and save the articles to a file.
        """
        args = self.parser.parse_args()
        start_date = datetime.datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end_date = datetime.datetime.strptime(args.end_date, "%Y-%m-%d").date()

        all_df = []
        n_days = (end_date - start_date).days + 1
        day_gen = enumerate(self.generate_dates(start_date=start_date, end_date=end_date))
        for i, date in tqdm(day_gen, total=n_days, desc="Scraping days"):
            try:
                all_df.append(self.get_articles(date))
            except:
                print(f"Error on date {date}")
            # save every 100 days
            if i % 100 == 0:
                self.save(pd.concat(all_df), args.output)
        self.save(pd.concat(all_df), args.output)
        print(f"Done - saved to {args.output}")


    def generate_dates(self, start_date: datetime.date = datetime.date(2018, 1, 1),
                       end_date: datetime.date = datetime.date.today()) -> str:
        """
        Generate a reverse chronological list of dates in the format YYYY-MM-DD.

        Keyword Arguments:
            start_date : datetime.date -- The start date (default: {datetime.date(2018, 1, 1)})
            end_date : datetime.date -- The end date (default: {datetime.date.today()})

        Yields:
            str -- The date in the format YYYY-MM-DD
        """
        day = end_date
        delta = datetime.timedelta(days=1)
        while day >= start_date:
            yield day.strftime("%Y-%m-%d")
            day -= delta

    @staticmethod
    def save(df, filename):
        """
        Save the DataFrame to a pickle or csv file.

        Arguments:
            df : pd.DataFrame -- The DataFrame
            filename : str -- The filename
        """
        if filename.endswith(".pkl"):
            df.to_pickle(filename)
        elif filename.endswith(".csv"):
            df.to_csv(filename, sep=",", index=False)
        else:
            df.to_pickle(filename + ".pkl")
            raise ValueError("Unknown file format, saved as pickle file.")

    @abstractmethod
    def get_articles(self, date: str) -> pd.DataFrame:
        r"""
        Parameters
        ----------
        date : str
            Date for which articles should be retrieved.

        Returns
        -------
        pandas DataFrame
            Articles published on that date.
        """

    def _setupparser(self) -> argparse.ArgumentParser:
        r"""
        :return: the Argument Parser instance
        """
        parser = argparse.ArgumentParser(description=f'Scrape the source: {self.sourcelabel}')
        parser.add_argument('--start_date', type=str, default="2018-01-01",
                            help='The start date in the format YYYY-MM-DD')
        parser.add_argument('--end_date', type=str, default=datetime.date.today().strftime("%Y-%m-%d"),
                            help='The end date in the format YYYY-MM-DD')
        parser.add_argument('--output', type=str, default="tagesschau.csv", help='The output file')
        return parser

    @property
    def sourcelabel(self) -> str:
        return self._sourcelabel

    @property
    def parser(self) -> argparse.ArgumentParser:
        return self._parser
