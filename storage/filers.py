from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship
from storage.sqlsession import Base


class FDICFiler(Base):
    __tablename__ = 'fdic_filers'
    cert_number = Column(Integer, primary_key=True, nullable=False)
    bank_name = Column(String(200))
    city = Column(String(100))
    state = Column(String(100))

    filings = relationship("FDICFiling", backref="fdic_filers")

    """ Returns a list of cert_numbers that already exist on the database."""
    @classmethod
    def get_local(cls, session):
        results = session.query(FDICFiler)
        return [r.cert_number for r in results]

    def __init__(self, cert_number, bank_name, city, state):
        self.cert_number = eval(cert_number)
        self.bank_name = bank_name
        self.city = city
        self.state = state

    def __repr__(self):
        return "<FDIC_Filer(cert_number=%d, bank_name='%s', city='%s', state='%s')>" % (
            self.cert_number, self.bank_name, self.city, self.state
        )