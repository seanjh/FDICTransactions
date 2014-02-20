import lxml.html
import requests
from sqlalchemy.exc import UnboundExecutionError
from storage.filers import FDICFiler
from scrape.scraper import FDICScraper


class FDICFilerScraper():

    def __init__(self):
        self.existing_certs = []

    def get_remote(self):
        # Request the page
        req = requests.get("http://www.fdic.gov/bank/individual/part335/index.html")
        tree = lxml.html.fromstring(req.text)

        # Parse headers from the HTML table
        table_th = [e for e in tree.body.iter('th')]
        table_headers = [e.text.strip() for e in table_th]

        # Parse the contents of the HMTL table (excluding header row)
        table_header_row = table_th[0].getparent()
        table_body_rows = [e for e in table_header_row.itersiblings()]

        # Transform table contents into dicts
        filers = []
        for row in table_body_rows:
            filers.append(dict(zip(table_headers, [e.text.strip() for e in row])))

        return filers

    def update(self, session):
        filers = self.get_remote()
        self._insert_new(session, filers)
        return filers

    def _insert_new(self, session, filers):
        if not self.existing_certs:
            self.existing_certs = FDICFiler.get_local(session)

        # Insert new FDIC filers results in the DB
        for f in filers:
            if eval(f.get("Cert Number")) not in self.existing_certs:
                filer = FDICFiler(
                    f.get("Cert Number"),
                    f.get("Bank Name"),
                    f.get("City"),
                    f.get("State")
                )

                try:
                    session.add(filer)
                except UnboundExecutionError as e:
                    print(e)

    def __repr__(self):
        return "<FDICFilerScraper()>"