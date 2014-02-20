from sqlalchemy import Column, Integer, String, ForeignKey, Date
from storage.sqlsession import Base


class FDICFiling(Base):
    __tablename__ = 'fdic_filings'

    disclosure_id = Column(Integer, primary_key=True)
    cert_number = Column(Integer, ForeignKey('fdic_filers.cert_number'))
    last_name = Column(String(100))
    first_name = Column(String(100))
    middle = Column(String(20))
    form_type = Column(String(10))
    filing_date = Column(Date)
    url = Column(String(200))

    """ Returns a list of disclosure_ids that already exist on the database."""
    @classmethod
    def get_local(cls, session):
        return session.query(FDICFiling)

    def __init__(self, cert_number, last_name, first_name, middle,
                 form_type, filing_date, disclosure_id, url):
        self.cert_number = cert_number
        self.last_name = last_name
        self.first_name = first_name
        self.middle = middle
        self.form_type = form_type
        self.filing_date = filing_date
        self.disclosure_id = disclosure_id
        self.url = url

    def __repr__(self):
        return ("<FDICFiling(cert_number='%d',last_name='%s',first_name='%s',"
                "middle=%s,form_type='%s',filing_date='%s',disclosure_id=%d,"
                "url='%s')>") % (
                self.cert_number, self.last_name, self.first_name,self.middle, self.form_type,
                self.filing_date, self.disclosure_id, self.url
        )