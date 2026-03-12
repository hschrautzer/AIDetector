r"""
Module contains data collector class for source tagesschau
"""
import json
import bs4.element
import pandas as pd
import bs4 as bs
import urllib.request
from datacollection.ICollector import ICollector
from multiprocessing import Pool
from typing import Union


class CTagesschauCollector(ICollector):
    r"""
    Data collector class for source tagesschau
    """

    # ------------------------------------ Initialization --------------------------------------------------------------
    def __init__(self) -> None:
        r"""
        Initializes the collector
        """
        super().__init__(sourcelabel="tagesschau")

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------ Implementation of abstract methods ------------------------------------------
    def get_articles(self, date: str) -> pd.DataFrame:
        """
        Retrieve articles for a given date.

        Parameters
        ----------
        date : str -- Date for which articles should be retrieved.

        Returns
        pandas DataFrame -- Articles published on that date.
        -------
        """
        children = self.load_content(date=date)
        headlines, short_headlines, short_text, links = self.filter_all(*self.get_metadata(children))
        articles = self.get_article_bodies_multiprocessing(links=links)
        df = pd.DataFrame(data={'date': [date] * len(headlines),
                                'headline': headlines,
                                'short_headline': short_headlines,
                                'short_text': short_text,
                                'article': articles,
                                'link': links})
        return df

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------ further methods -------------------------------------------------------------

    def get_article_bodies_multiprocessing(self, links):
        with Pool(8) as p:
            return p.map(self.get_article_body, links)

    def get_metadata(self, children):
        """
        Get the metadata of the articles from the children of the content div.

        Arguments:
            children {list} -- List of children of the content div

        Returns:
            list -- List of headlines
            list -- List of short headlines
            list -- List of short texts
            list -- List of links
        """
        headlines = self.find_for_all(children, "span", "teaser-right__headline")
        short_headlines = self.find_for_all(children, "span", "teaser-right__labeltopline")
        short_text = self.find_for_all(children, "p", "teaser-right__shorttext")
        links = self.find_for_all(children, "a", "teaser-right__link")
        return headlines, short_headlines, short_text, links

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------ static methods --------------------------------------------------------------
    @staticmethod
    def get_article_body(href) -> Union[None, str]:
        """
        Get the article body from the href of the article. The HTML contains a script tag with the article body in
        JSON format.

        Arguments:
            href : str -- The href of the article

        Returns:
            str -- The article body
            """
        try:
            url = "https://www.tagesschau.de" + href
            source = urllib.request.urlopen(url).read()
            soup = bs.BeautifulSoup(source, features="html.parser")
            scripts = soup.findAll('script', attrs={'type': 'application/ld+json'})
            for script in scripts:
                data = json.loads(script.text)
                if data["@type"] == "NewsArticle":
                    return data["articleBody"]
            return None
        except:
            return None

    @staticmethod
    def load_content(date: str) -> bs4.element.ResultSet:
        """
        Load the content of the tagesschau.de archive for a given date.

        Arguments:
            date : str -- The date in the format YYYY-MM-DD

        Returns:
            list -- List of children of the content div
        """
        # url format url + ?datum=2018-07-01
        url = "https://www.tagesschau.de/archiv?datum=" + date
        source = urllib.request.urlopen(url).read()
        soup = bs.BeautifulSoup(source, features="html.parser")
        content = soup.find('div', attrs={'id': 'content'})
        return content.find_all("div", class_='copytext-element-wrapper__vertical-only')

    @staticmethod
    def filter_all(headlines: list, short_headlines, short_text, links) -> tuple[list, list, list, list]:
        """
        Filter out all articles that are not from the tagesschau.de domain.

        Arguments:
            headlines : List -- List of headlines
            short_headlines : List -- List of short headlines
            short_text : List -- List of short texts
            links : List -- List of links

        Returns:
            list -- List of filtered headlines
            list -- List of filtered short headlines
            list -- List of filtered short texts
            list -- List of filtered links
        """
        indices = [i for i, x in enumerate(links) if x.startswith("/")]
        headlines = [headlines[i] for i in indices]
        short_headlines = [short_headlines[i] for i in indices]
        short_text = [short_text[i] for i in indices]
        links = [links[i] for i in indices]
        return headlines, short_headlines, short_text, links

    @staticmethod
    def find_for_all(children: bs4.element.ResultSet, attr: str, value: str):
        """Find the value of an attribute for all children of a given list of children.

        Arguments:
            children : list -- List of children
            attr : str -- The attribute to find
            value : str -- The value of the attribute

        Returns:
            list -- List of values
        """
        out = []
        for child in children[2:]:
            val = child.find(attr, attrs={'class': value})
            if val is not None:
                if attr == "a":
                    out.append(val['href'])
                else:
                    out.append(val.text)
            else:
                out.append("")
        return out
    # ------------------------------------------------------------------------------------------------------------------
