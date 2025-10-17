from sqlalchemy import MetaData, Table
from .db import engine

metadata = MetaData()
leads = Table("leads", metadata, autoload_with=engine)
email_sends = Table("email_sends", metadata, autoload_with=engine)
