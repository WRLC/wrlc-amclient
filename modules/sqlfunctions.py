import settings
import database


def get_collection_data(islandora_pid):
    sql = 'SELECT * FROM collection WHERE pid = %s'
    val = (islandora_pid,)

    with database.Database(settings.database) as connection:
        result = connection.sql_select(sql, val)
        return result


# Function to insert AIP row into PAWDB
def aip_row(fields):
    # Define SQL query
    sql = 'INSERT INTO aip (uuid, dateCreated, pipelineURI, resourceURI, stgFullPath, rootCollection) VALUES (' \
          '%s, %s, %s, %s, %s, %s)'

    # Define data values
    val = (
        fields['uuid'],
        fields['datecreated'],
        fields['pipelineuri'],
        fields['resourceuri'],
        fields['stgfullpath'],
        fields['rootcollection']
    )

    # Insert DB row
    with database.Database(settings.database) as connection:
        connection.insert_row(sql, val)


# Function to insert Collection row into PAWDB
def collection_row(fields):
    # Define SQL query
    sql = 'INSERT INTO collection (pid, label, parentCollection) VALUES (' \
          '%s, %s, %s)'

    # Define data values
    val = (
        fields['pid'],
        fields['label'],
        fields['parent']
    )

    # Insert DB row
    with database.Database(settings.database) as connection:
        connection.insert_row(sql, val)


# Function to insert Collection row into PAWDB
def object_row(fields):
    # Define SQL query
    sql = 'INSERT INTO object (pid, label, identifierURI, identifierLocal, seqNumber, parent, aipUUID) VALUES (' \
          '%s, %s, %s, %s, %s, %s, %s)'

    # Define data values
    val = (
        fields['pid'],
        fields['label'],
        fields['identifierURI'],
        fields['identifierLocal'],
        fields['seqNumber'],
        fields['parent'],
        fields['aipUUID']
    )

    # Insert DB row
    with database.Database(settings.database) as connection:
        connection.insert_row(sql, val)
