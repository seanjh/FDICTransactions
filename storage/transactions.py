from sqlalchemy import Column, Integer, String, ForeignKey, Date
from sqlalchemy.dialects.mssql import MONEY, BIT
from storage.sqlsession import Base
from datetime import datetime


class FDICTradeHandler():

    DATE_FORMAT = "%m/%d/%Y"

    @classmethod
    def get_existing_discl_ids(cls, session):
        """Return a list of disclosure_id values in the local database"""
        return (session.query(FDICTransFilingInfo.disclosure_id)).union(
            session.query(FDICTransFilerInfo.disclosure_id)
        ).all()




    @classmethod
    def map_columns(cls, keyword_map, row_data):
        """Associates keywords, each corresponding to data items, with the full column
         name parsed from the source document."""
        for keyword in keyword_map.keys():
            for column in row_data.keys():
                if keyword in column:
                    keyword_map[keyword] = column
                    break
        return keyword_map

    @classmethod
    def parse_shares(cls, value_string):
        """Returns all the numbers from a string as a single consolidated integer"""
        if value_string:
            shares_chars = [ch for ch in value_string if ch.isnumeric()]
            if shares_chars:
                return int(''.join(shares_chars))

    @classmethod
    def parse_acq(cls, value_string):
        """Returns the acquisition/disposition flag appearing in parens beside Share values (e.g. (A))"""
        if value_string:
            for i, ch in value_string:
                if value_string[i - 1] == "(":
                    return ch

    @classmethod
    def parse_price(cls, value_string):
        """Returns a float/money number from an input string"""
        if value_string:
            price_chars = [ch for ch in value_string if ch.isnumeric() or ch == '.']
            if price_chars:
                return float(''.join(price_chars))

    @classmethod
    def parse_text(cls, value_string, max_length=None):
        """Returns None for blank strings, otherwise returns a string limited by max_length (if necessary)"""
        if value_string:
            if max_length:
                return value_string[0:max_length]
            else:
                return value_string
        else:
            return None


class FDICTransFilerInfo(Base):
    __tablename__ = 'fdic_trans_filer_info'

    id = Column(Integer, primary_key=True)
    disclosure_id = Column(Integer, ForeignKey("fdic_filings.disclosure_id"), nullable=False,)
    info_number = Column(Integer)
    title = Column(String(100))
    name = Column(String(100))
    city = Column(String(100))
    state = Column(String(25))
    street = Column(String(100))
    zip = Column(String(20))

    def __init__(self, disclosure_id, info_number, row_data):
        self._raw_row_data = row_data
        self.disclosure_id = disclosure_id
        self.info_number = info_number

        # Map attributes to the matching ket in row_data
        self._keyword_map = {
            "Relationship": None,
            "Name": None,
            "City": None,
            "State": None,
            "Street": None,
            "ZIP": None
        }
        FDICTradeHandler.map_columns(self._keyword_map, row_data)

        # Update the attribute using the keyword map
        self.title = FDICTradeHandler.parse_text(row_data.get(self._keyword_map.get("Relationship")), 100)
        self.name = FDICTradeHandler.parse_text(row_data.get(self._keyword_map.get("Name")), 100)
        self.city = FDICTradeHandler.parse_text(row_data.get(self._keyword_map.get("City")), 100)
        self.state = FDICTradeHandler.parse_text(row_data.get(self._keyword_map.get("State")), 25)
        self.street = FDICTradeHandler.parse_text(row_data.get(self._keyword_map.get("Street")), 100)
        self.zip = FDICTradeHandler.parse_text(row_data.get(self._keyword_map.get("ZIP")), 20)

    def __repr__(self):
        return ("<FDICTransFilerInfo(disclosure_id=%d, info_number=%d,"
                "row_date=%s)>" %
                (self.disclosure_id, self.info_number, self._raw_row_data)
                )


class FDICTransFilingInfo(Base):
    __tablename__ = 'fdic_trans_issuer_info'

    id = Column(Integer, primary_key=True)
    disclosure_id = Column(Integer, ForeignKey("fdic_filings.disclosure_id"), nullable=False,)
    info_number = Column(Integer)
    issuer_name = Column(String(100))
    issuer_ticker = Column(String(20))
    report_date = Column(Date)
    amendment_date = Column(Date)

    def __init__(self, disclosure_id, info_number, row_data):
        self._raw_row_data = row_data
        self.disclosure_id = disclosure_id
        self.info_number = info_number

        self._keyword_map = {
            "Name": None,
            "Earliest": None,
            "Event": None,
            "Ticker": None,
            "Amendment": None
        }
        FDICTradeHandler.map_columns(self._keyword_map, row_data)

        self.issuer_name = row_data.get(self._keyword_map.get("Name"))[0:100]
        self.issuer_ticker = row_data.get(self._keyword_map.get("Ticker"))[0:20]

        # Parse out the date values
        if self._keyword_map.get("Earliest"):
            date_string = row_data.get(self._keyword_map.get("Earliest"))
        else:
            date_string = row_data.get(self._keyword_map.get("Event"))

        if date_string:
            self.report_date = datetime.strptime(date_string, FDICTradeHandler.DATE_FORMAT)
        else:
            self.report_date = None

        date_string = row_data.get(self._keyword_map.get("Amendment"))
        if date_string:
            self.amendment_date = datetime.strptime(date_string, FDICTradeHandler.DATE_FORMAT)
        else:
            self.amendment_date = None

    def __repr__(self):
        return ("<FDICTransFilingInfo(disclosure_id=%d, info_number=%d,"
                "row_data=%s)>" %
                (self.disclosure_id, self.info_number, self._raw_row_data)
                )


class FDICTransTrade(Base):
    __tablename__ = 'fdic_trans_trades'

    id = Column(Integer, primary_key=True)
    disclosure_id = Column(Integer, ForeignKey("fdic_filings.disclosure_id"), nullable=False)
    trade_number = Column(Integer)
    _security = Column('security', String(100))
    _trade_date = Column('trade_date', Date)
    _exec_date = Column('exec_date', Date)
    _code = Column('code', String(10))
    _v_flag = Column('v_flag', BIT)
    _trade_shares = Column('trade_shares', Integer)
    _trade_acq = Column('trade_acq', BIT)
    _trade_price = Column('trade_price', MONEY)
    _shares_owned = Column('shares_owned', Integer)
    _direct_own = Column('direct_own', BIT)
    _nature_of_own = Column('nature_of_own', String(100))
    derivative = Column(BIT)
    _exercise_price = Column('exercise_price', MONEY)
    _exercise_date = Column('exercise_date', Date)
    _expire_date = Column('expire_date', Date)
    _underlying_security = Column('underlying_security', String(100))
    _underlying_shares = Column('underlying_shares', Integer)

    """ Returns a list of disclosure_ids that already exist in the table."""
    @classmethod
    def get_local_discl(cls, session):
        results = session.query(FDICTransTrade)
        return [r.disclosure_id for r in results]

    def __init__(self, disclosure_id, trade_number, row_data, derivative=False):
        # TODO Form 3 "Ownership" column where "Owership Form" usually appears
        # TODO Form 3 derivative "Amount of Securities Underlying Derivative Security" differs
        # http://www2.fdic.gov/efr/redirect.asp?Discl_id=847&InstNme=&InstCty=&CertNum=35095&InstSte=&sGoto=Institution
        self._raw_row_data = row_data
        self.disclosure_id = int(disclosure_id)
        self.trade_number = trade_number
        self.derivative = derivative

        self._keyword_map = {}
        self.initialize_keyword_map()
        FDICTradeHandler.map_columns(self._keyword_map, row_data)

        # Common columns
        self.trade_date = row_data.get(self._keyword_map.get("Transaction Date"))
        self.code = row_data.get(self._keyword_map.get("Code"))
        self.exec_date = row_data.get(self._keyword_map.get("Execution Date"))
        self.direct_own = row_data.get(self._keyword_map.get("Form"))
        self.shares_owned = row_data.get(self._keyword_map.get("Beneficially Owned"))
        self.nature_of_own = row_data.get(self._keyword_map.get("Nature of"))
        self.v_flag = row_data.get("V")

        # Table-specific columns
        if derivative:
            self.security = row_data.get(self._keyword_map.get("Title of Derivative Security"))
            self.exercise_price = row_data.get(self._keyword_map.get("Exercise Price"))
            self.trade_shares = row_data.get(self._keyword_map.get("Derivative Securities Acquired"))
            self.exercise_date = row_data.get(self._keyword_map.get("Exercisable"))
            self.expire_date = row_data.get(self._keyword_map.get("Expiration"))
            self.underlying_security = row_data.get(self._keyword_map.get("Title of Underlying Securities"))
            self.underlying_shares = row_data.get(self._keyword_map.get("Amount of Underlying Securities"))
            self.trade_price = row_data.get(self._keyword_map.get("Price of Derivative Security"))
        else:
            self.trade_shares = row_data.get(self._keyword_map.get("Amount of Securities Acquired"))
            self.security = row_data.get(self._keyword_map.get("Title of Security"))
            self.trade_price = row_data.get(self._keyword_map.get("Price of Securities Acquired"))

        # Form 3 type filings provide a bad header for "Security" for Non-Derivative trades.
        # This exception locates and pulls the appropriate data in these cases.
        if not self.security and not self.derivative:
            goofy_header = ''.join([head for head in row_data.keys() if 'Title of' in head])
            self.security = row_data.get(goofy_header)

    def initialize_keyword_map(self):
        # Keywords common to Table 1 (Non-derivative) and Table 2 (derivative)
        self._keyword_map["Transaction Date"] = None
        self._keyword_map["Code"] = None
        self._keyword_map["Execution Date"] = None
        self._keyword_map["Form"] = None
        self._keyword_map["Beneficially Owned"] = None
        self._keyword_map["Nature of"] = None

        if self.derivative:  # derivative keywords
            self._keyword_map["Title of Derivative Security"] = None
            self._keyword_map["Exercise Price"] = None
            self._keyword_map["Derivative Securities Acquired"] = None
            self._keyword_map["Exercisable"] = None
            self._keyword_map["Expiration "] = None
            self._keyword_map["Title of Underlying Securities"] = None
            self._keyword_map["Amount of Underlying Securities"] = None
            self._keyword_map["Price of Derivative Security"] = None
        else:  # Non-derivative keywords
            self._keyword_map["Amount of Securities Acquired"] = None
            self._keyword_map["Title of Security"] = None
            self._keyword_map["Price of Securities Acquired"] = None

    def __repr__(self):
        return ("<FDICTransTrade(disclosure_id=%d,trade_number=%d,"
                "row_data=%s,derivative=%s)>" % (self.disclosure_id, self.trade_number, self.row, self.derivative))

    @property
    def trade_shares(self):
        return self._trade_shares

    @trade_shares.setter
    def trade_shares(self, value_string):
        self._trade_shares = FDICTradeHandler.parse_shares(value_string)

    @property
    def trade_date(self):
        return self._trade_date

    @trade_date.setter
    def trade_date(self, value_string):
        self._trade_date = FDICTradeHandler.parse_text(value_string)

    @property
    def security(self):
        return self._security

    @security.setter
    def security(self, value_string):
        self._security = FDICTradeHandler.parse_text(value_string, 100)

    @property
    def code(self):
        return self._code

    @code.setter
    def code(self, value_string):
        self._code = FDICTradeHandler.parse_text(value_string, 10)

    @property
    def exec_date(self):
        return self._exec_date

    @exec_date.setter
    def exec_date(self, value_string):
        self._exec_date = FDICTradeHandler.parse_text(value_string)

    @property
    def shares_owned(self):
        return self._shares_owned

    @shares_owned.setter
    def shares_owned(self, value_string):
        self._shares_owned = FDICTradeHandler.parse_shares(value_string)

    @property
    def nature_of_own(self):
        return self._nature_of_own

    @nature_of_own.setter
    def nature_of_own(self, value_string):
        self._nature_of_own = FDICTradeHandler.parse_text(value_string, 100)

    @property
    def underlying_shares(self):
        return self._underlying_shares

    @underlying_shares.setter
    def underlying_shares(self, value_string):
        self._underlying_shares = FDICTradeHandler.parse_shares(value_string)

    @property
    def underlying_security(self):
        return self._underlying_security

    @underlying_security.setter
    def underlying_security(self, value_string):
            self._underlying_security = FDICTradeHandler.parse_text(value_string, 100)

    @property
    def direct_own(self):
        return self._direct_own

    @direct_own.setter
    def direct_own(self, value_string):
        if value_string:
            if "Indirect" in value_string:
                self._direct_own = False
            else:
                self._direct_own = True

    @property
    def trade_price(self):
        return self._trade_price

    @trade_price.setter
    def trade_price(self, value_string):
        self._trade_price = FDICTradeHandler.parse_price(value_string)

    @property
    def exercise_price(self):
        return self._exercise_price

    @exercise_price.setter
    def exercise_price(self, value_string):
        self._exercise_price = FDICTradeHandler.parse_price(value_string)

    @property
    def exercise_date(self):
        return self._exercise_date

    @exercise_date.setter
    def exercise_date(self, value_string):
        self._exercise_date = FDICTradeHandler.parse_text(value_string)

    @property
    def expire_date(self):
        return self._expire_date

    @expire_date.setter
    def expire_date(self, value_string):
        self._expire_date = FDICTradeHandler.parse_text(value_string)

    @property
    def v_flag(self):
        return self._v_flag

    @v_flag.setter
    def v_flag(self, value_string):
        if value_string:
            if "V" in value_string:
                self._v_flag = True
        else:
            self._v_flag = False


class FDICTransNotes(Base):
    __tablename__ = 'fdic_trans_notes'

    id = Column(Integer, primary_key=True)
    disclosure_id = Column(Integer, ForeignKey("fdic_filings.disclosure_id"), nullable=False)
    note_number = Column(Integer)
    footnote = Column(String(2500))

    def __init__(self, disclosure_id, note_number, row_data):
        self.disclosure_id = disclosure_id
        self.note_number = note_number
        self.footnote = FDICTradeHandler.parse_text(row_data, 2500)

    def __repr__(self):
        return ("<FDICTransNotes(disclosure_id=%d,note_number=%d, row_data=%s)>" %
                (self.disclosure_id, self.note_number, self.row_data))