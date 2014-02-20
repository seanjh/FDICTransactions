import os
import sys
from sqlalchemy import create_engine
from storage.sqlsession import session_scope, Base
from scrape.scrape_filers import FDICFilerScraper
from scrape.scrape_listing import FDICOwnFilingScraper
from scrape.scrape_trades import FDICInsiderFileScraper
from storage.transactions import FDICTradeHandler


def get_mssql_engine():
    try:
        config_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), "settings.cfg")
        with open(config_file, 'r') as infile:
            settings = dict([line.strip().split("=") for line in infile.readlines()])
    except FileNotFoundError as e:
        settings = None
        filename = create_blank_settings_file()
        print("Required settings.cfg file is missing. A blank settings.cfg has been created at %s."  % filename)
        print("Input server name and table name and retry.")
        exit(0)

    connection_string = "mssql+pyodbc://%s/%s" % (settings.get("database"), settings.get("table"))
    return create_engine(connection_string, echo=True)

def create_blank_settings_file():
    if hasattr(sys, "frozen"):
        path = os.path.dirname(sys.executable)
    else:
        path = os.path.dirname(__file__)
    filename = os.path.join(path, "settings.cfg")

    with open(os.path.join(path, "settings.cfg"), "w") as outfile:
        outfile.write("database=")
        outfile.write("table=")

    return filename

def main():
    engine = get_mssql_engine()
    Base.metadata.create_all(engine)
    with session_scope(engine) as session:
        # Scrape the list of filers
        f0 = FDICFilerScraper()
        filers = f0.update(session)

        # Scrape the file listing for each filer
        f1 = FDICOwnFilingScraper()
        #f1.update(session, "35095")
        for filer in filers:
            f1.update(session, filer.get("Cert Number"))

        # Commit before fetching files to ensure the disclosure IDs are in the DB.
        # The underlying table/trade data have FK references that depend on these disclosure IDs
        session.commit()

        # From the full file listing, identify those that do not exist on the DB
        existing_discl_ids = FDICTradeHandler.get_existing_discl_ids(session)
        new_urls = f1.get_new_urls(session, [item for (item,) in existing_discl_ids])
        for i, url in enumerate(new_urls):
            print("%d new files identified. Beginning scrape.")
            sys.stdout.write("\rRequesting file #%d/%d @ %s" % (i + 1, len(new_urls), url))
            sys.stdout.flush()

            # Scrape the table
            f2 = FDICInsiderFileScraper(url)
            f2.update(session)


if __name__ == '__main__':
    main()