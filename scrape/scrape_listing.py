from urllib.parse import urlparse, parse_qs
import lxml.html
import requests
from sqlalchemy.exc import UnboundExecutionError
from storage.file_listing import FDICFiling


class FDICOwnFilingScraper():

    BASE_URL = 'http://www2.fdic.gov/efr/instdetail.asp'

    def __init__(self):
        self.existing_filings = []
        self.new_filings = []

    def get_remote(self, cert_number):
        """Return a dict containing the file listing table for the given cert number"""
        payload = {'CertNum': str(cert_number),
                   'CertNum_INTEGER': 'The FDIC Certificate Number must req.,be a positive integer'}
        req = requests.post(FDICOwnFilingScraper.BASE_URL, params=payload)
        tree = lxml.html.fromstring(req.text, base_url=FDICOwnFilingScraper.BASE_URL)
        tree.make_links_absolute()

        # Parse header TH elements out of the tree
        table_headers = [''.join(th.xpath('.//text()')) for th in tree.xpath('body//table[@class]//tr/th')]
        # Supplement the displayed headers with 2 additional columns: URL and Disclosure ID
        table_headers.append("URL")
        table_headers.append("Disclosure ID")

        # Compose a list of rows with content (i.e., TD elements)
        rows = [row for row in tree.xpath('body//table[@class]//tr') if row.xpath('.//td')]

        table_content = []
        for row in rows:
            # Expand the row into a list of TDs
            contents = row.xpath('.//td')

            # Translate each row from a list of TDs to a list of sanitized text
            one_row = [' '.join(''.join(td.xpath('.//text()')).split()) for td in contents]
            unique = True

            # Append the embedded URL and Discl_id (included in URL)
            if row.xpath('.//a'):
                url = row.xpath('.//a')[-1].get('href')
                one_row.append(url)
                discl_id = FDICOwnFilingScraper.parse_url_discl_id(url)
                one_row.append(discl_id)
                if discl_id in [r.get("Disclosure ID") for r in table_content]:
                    unique = False
            else:
                one_row.append(None)
                one_row.append(None)

            if unique:
                table_content.append(dict(zip(table_headers, one_row)))

        return table_content

    def update(self, session, cert):
        """Get the table from FDIC.gov, and insert new files on the list in the DB"""
        filings = self.get_remote(cert)
        self._insert_new(session, filings, cert)
        return filings

    def _insert_new(self, session, filings, cert):
        if not self.existing_filings:
            self.existing_filings = [f.disclosure_id for f in FDICFiling.get_local(session)]

        # For new files, create FDICFiling objects for insertion into the DB.
        for file in filings:
            try:
                if eval(file.get("Disclosure ID")) not in self.existing_filings:
                    self.new_filings.append(file)
                    filing = FDICFiling(
                        cert, file.get("Last Name"), file.get("First Name"),
                        file.get("Middle Initial"), file.get("Form Name"),
                        file.get("Filing Date"), file.get("Disclosure ID"),
                        file.get("URL")
                    )

                    try:
                        session.add(filing)
                    except UnboundExecutionError as e:
                        print(e)

            except TypeError as e:
                print(e)

    @classmethod
    def parse_url_discl_id(cls, url):
        """Returns the discl_id parameter from an input URL"""
        url_query = urlparse(url)[4]
        try:
            return parse_qs(url_query).get('Discl_id', None)[-1]
        except IndexError as e:
            print(e)
            return ""

    @classmethod
    def parse_url_certnum(cls, url):
        """Returns the cert_num parameter from an input URL"""
        url_query = urlparse(url)[4]
        try:
            return parse_qs(url_query).get('CertNum', None)[-1]
        except IndexError as e:
            print(e)
            return ""

    @classmethod
    def get_new_urls(cls, session, discl_ids):
        """Returns a list of URLs from discl_ids that do not already exist in the DB."""
        local_discl = FDICFiling.get_local(session)
        return [file.url for file in local_discl if file.disclosure_id not in discl_ids]

    def __repr__(self):
        return "<FDICOwnFilingScraper(cert)>"