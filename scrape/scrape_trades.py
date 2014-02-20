import lxml.html
import requests
from scrape.scrape_listing import FDICOwnFilingScraper
from storage.transactions import FDICTransFilerInfo, FDICTransFilingInfo, FDICTransTrade, FDICTransNotes


def test_scrape():
    url = ('http://www2.fdic.gov/efr/redirect.asp?Discl_id=12909&InstNme=&InstCty=&CertNum=35095&InstSte=&sGoto=Institution')
    #url = FDICInsiderFileScraper.compose_url(58481, 21978)
    print(url)
    f = FDICInsiderFileScraper(url)
    f.get_remote()

    for section in FDICInsiderFileScraper.SECTIONS:
        if f.table_data.get(section):
            print(section + ":")
            for i, line in enumerate(f.table_data.get(section)):
                print("\t", end="")
                print(i, str(line))
    #file.update()


class FDICInsiderFileScraper():

    SECTIONS = (['Filing Information', 'Filer Information',
                'Table I - Non-Derivative', 'Table II - Derivative',
                'Explanation of Responses', 'Exhibit Information', 'EOF'])

    @classmethod
    def compose_url(cls, cert_number, disclosure_id):
        return ("http://www2.fdic.gov/efr/redirect.asp?Discl_id=%s&InstNme="
                "&InstCty=&CertNum=%s&InstSte=&sGoto=Institution") % (str(disclosure_id), str(cert_number))

    @classmethod
    def get_existing_dicl(cls, session):
        return FDICTransTrade.get_local_discl(session)

    def __init__(self, url):
        self.url = url
        self.disclosure_id = FDICOwnFilingScraper.parse_url_discl_id(url)
        self.cert_number = FDICOwnFilingScraper.parse_url_certnum(url)
        self.table_data = None

    def update(self, session):
        self.get_remote()
        #print(self.url)

        section = self.table_data.get("Filing Information")
        if section:
            for i, row in enumerate(section):
                session.add(FDICTransFilingInfo(self.disclosure_id, i + 1, row))

        section = self.table_data.get("Filer Information")
        if section:
            for i, row in enumerate(section):
                session.add(FDICTransFilerInfo(self.disclosure_id, i + 1, row))

        section = self.table_data.get("Table I - Non-Derivative")
        row_counter = 0
        if section:
            for i, row in enumerate(section):
                if row and 'There are no' not in row:  # Skip blank entries
                    row_counter += 1
                    session.add(FDICTransTrade(self.disclosure_id, row_counter, row))

        # row_counter continues between Table I and Table II
        section = self.table_data.get("Table II - Derivative")
        if section:
            for i, row in enumerate(section):
                if row and 'There are no' not in row:  # Skip blank entries
                    row_counter += 1
                    session.add(FDICTransTrade(self.disclosure_id, row_counter, row, derivative=True))

        # Reset row_counter for notes
        row_counter = 0
        section = self.table_data.get("Explanation of Responses")
        # Exclude last 2 Explanation rows.
        # These 2 lines are always: 1) signature line and 2) junk legalese
        if section[0:-2]:
            for i, row in enumerate(section[0:-2]):
                if row:  # Skip blank entries
                    row_counter += 1
                    session.add(FDICTransNotes(self.disclosure_id, row_counter, row))

    def get_remote(self):
        self.table_data = self._parse_table(self.url)

    @classmethod
    def _parse_table(cls, url):
        req = requests.get(url)
        if req.ok:
            tree = lxml.html.fromstring(req.text)
        else:
            raise requests.ConnectionError

        # List of rows (TR elements) inside the relevant table
        table_rows = FDICInsiderFileScraper._get_table_rows(tree)

        # Build an index of the row numbers for each section heading
        section_index = FDICInsiderFileScraper._index_sections(table_rows, FDICInsiderFileScraper.SECTIONS)

        table_data = {}
        # Compose data for each section from the range of relevant rows in section_index
        for i in range(len(FDICInsiderFileScraper.SECTIONS)-1):
            # Relevant rows range from this section's to the next section's row index
            start, end = (section_index[FDICInsiderFileScraper.SECTIONS[i]],
                          section_index[FDICInsiderFileScraper.SECTIONS[i+1]])

            # Returns list, list of dicts, or None for this section
            table_data[FDICInsiderFileScraper.SECTIONS[i]] = (
                FDICInsiderFileScraper._compose_section(table_rows[start: end])
            )

        # Determine whether the exit filing indicator is checked
        table_data['Exit Filing'] = FDICInsiderFileScraper._pull_exit_checkbox(table_rows)

        return table_data

    @classmethod
    def _get_table_rows(cls, tree):
        # Return the visible table (i.e., not just used for layout purposes)
        return tree.xpath('body/table/tr/td/table[@border="1"]/tr')

    @classmethod
    def _index_sections(cls, rows, sections):
        section_index = {}
        last_head = 0

        for head in sections:
            # Find this head's row number in relation to the last head's row number.
            sub_index = FDICInsiderFileScraper._get_row_number(head, rows[last_head:])
            if sub_index is not None:
                last_head, index = sub_index, (last_head + sub_index)
                section_index[head] = index
                last_head = index

        # Exhibit defaults to the last row when absent
        section_index.setdefault('Exhibit Information', len(rows))
        section_index.setdefault('EOF', len(rows))

        return section_index

    @classmethod
    def _get_row_number(cls, head_text, rows):
        for row_num, tr in enumerate(rows):
                # Cells/TD elements in the column
                columns = tr.xpath('td')

                # Return and pop the cell whose text matches head_text
                # Header rows are 1 long column, so only check the first column for matches
                if columns:
                    if head_text in ' '.join((FDICInsiderFileScraper._all_text_below(columns[-1]).split())):
                        rows.pop(row_num)
                        return row_num

    @classmethod
    def _all_text_below(cls, element):
        # Returns all text contained within and below this element
        return ' '.join(element.xpath('.//text()'))

    @classmethod
    def _compose_section(cls, rows):
        # The first row is the section header, and is not needed.
        if rows:
            rows.pop(0)

        # The column headers (if present) are within a row's nested th elements
        headers = FDICInsiderFileScraper._get_column_headers(rows, 'th')

        if headers:
            # Returns a list of dicts pairing each datum with its header
            return FDICInsiderFileScraper._compose_labelled(headers, rows)
        else:
            # Returns a plain list, since there are no column headers for pairing
            return FDICInsiderFileScraper._compose_unlabelled(rows)

    @classmethod
    def _get_column_headers(cls, rows, tag):
        # Get the contents for a row as a list
        for i, row in enumerate(rows):
            column_data = row.xpath(tag + '//text()')
            if column_data:
                for j, column in enumerate(column_data):
                    column_data[j] = ' '.join(column.split())
                rows.pop(i)
                return column_data

    @classmethod
    def _compose_labelled(cls, headers, rows):
        section = []
        for i, row in enumerate(rows):
            one_row = FDICInsiderFileScraper._get_row_contents(row, 'td')
            if one_row:
                if len(one_row) == len(headers):
                    section.append(dict(zip(headers, one_row)))
            else:
                pass  # Nothing to append
        return section

    @classmethod
    def _compose_unlabelled(cls, rows):
        section = []
        for i, row in enumerate(rows):
            # Pull the list of row contents into a single string
            section.append(''.join(FDICInsiderFileScraper._get_row_contents(row, 'td')))
        return section

    @classmethod
    def _get_row_contents(cls, row, tag):
        # Returns a list of the row's contents.
        # Each piece consists of all the text within or below that element (i.e., td/th)
        contents = [FDICInsiderFileScraper._all_text_below(e) for e in row.xpath(tag)]
        return [' '.join(td_text.split()) for td_text in contents]

    @classmethod
    def _pull_exit_checkbox(cls, trs):
        # Locates and gets the "exit filing" checkbox from the HTML
        for i, row in enumerate(trs):
            exit_inputs = row.xpath('td/input[@type]')
            if exit_inputs:
                #_pop_table_row(trs, table_index, i)
                if [e for e in exit_inputs if 'checked' in e.attrib]:
                    return True
                else:
                    return False

if __name__ == '__main__':
    test_scrape()