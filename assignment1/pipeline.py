import psycopg2
import json
import sys
from typing import Dict, Tuple, List
from lxml import etree

CellRepresentation = Tuple[str, str]  # structure: ('column_name', 'cell_value')
RowRepresentation = List[CellRepresentation] # structure: [('column_name', 'cell_value'), ...]
AuthorshipListing = List[str]

relevant_tags = {
    'article': {'title', 'journal', 'year'},
    'inproceedings': {'title', 'booktitle', 'year'}
}

def get_host() -> str:
    """Gets the first argument passed to the command line. Needed to get IP of Windows Postgres for access by WSL2"""
    return sys.argv[1]

def read_config() -> Dict[str, str]:
    f = open('config.json', "r")
    config = json.loads(f.read())
    if 'host' not in config or config['host'] == "":
        config['host'] = get_host()
    return config

def connect():
    """Connects to a database based on the configuration files. Returns connection cursor"""
    # guided by this stackoverflow post: https://stackoverflow.com/questions/35308623/in-pythons-elementree-library-how-to-use-iterparse-only-for-the-outer-level
    conn = None
    try:
        conf = read_config()
        print('Connecting to Postgres')
        conn = psycopg2.connect(**conf)
        print('Connection established')
        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        
def get_pubkey(elem) -> str:
    """Returns the primary key element from a tag"""
    return elem.get('key')

def get_text(elem) -> str:
    """Returns the inner text of a tag"""
    return "".join(elem.itertext())

def build_column_tuple(elem) -> CellRepresentation:
    """Returns a tuple of the element's tag name and text"""
    return (elem.tag, get_text(elem))
    

def clean_single_quotes(text: str) -> str:
    """Excapes all single quotes (') in text into a double single quote('')"""
    return text.replace("'", "''")

def handle_tag_type(elem) -> Tuple[RowRepresentation, AuthorshipListing, str]:
    """Returns the data we want for either inproceeding or article rows"""
    lookout_tags = relevant_tags.get(elem.tag)
    if lookout_tags is None:
        # this shouldn't happen, but just in case
        raise ValueError(f'Incompatible tag of type {elem.tag} passed')
    pubkey = clean_single_quotes(get_pubkey(elem))
    row = [('pubkey', pubkey)]
    authors = []
    for child in elem:
        if child.tag in lookout_tags:
            row.append(build_column_tuple(child))
        elif child.tag == 'author':
            authors.append(get_text(child))
    return (row, authors, pubkey)

def build_row_names(row: RowRepresentation) -> str:
    """Joins all row names (first el of cell tuples) into valid SQL"""
    row_names = [clean_single_quotes(el[0]) for el in row]
    return ', '.join(row_names)

def build_row_values(row: RowRepresentation) -> str:
    """Joins all row values (second el of cell tuples) into valid SQL"""
    row_vals = []
    for name, val in row:
        if name == 'year':
            # year is an integer and doesn't need quotes
            row_vals.append(f"{val}")
        else:
            row_vals.append(f"'{clean_single_quotes(val)}'")
            
    return ', '.join(row_vals)
    
def build_item_insert(row: RowRepresentation, table_name: str) -> str:
    """Turns a row representation into a SQL query for a given table name"""
    # table names are not capitalized
    return f"INSERT INTO public.{table_name} ({build_row_names(row)}) VALUES ({build_row_values(row)});"
    
def dedup_authorships(authors: AuthorshipListing) -> AuthorshipListing:
    """Removes duplicates from author lists"""
    return list(set(authors))

def build_author_values(authors: AuthorshipListing, pubkey: str) -> str:
    """"Turns list of authors into values section for multiple insert"""
    keyed_authors = [f"('{pubkey}', '{clean_single_quotes(a)}')" for a in dedup_authorships(authors)]
    return ', '.join(keyed_authors)

def build_authors_insert(authors: AuthorshipListing, pubkey: str) -> str:
    """Turns a list of authors into a SQL query to insert them into authorship table"""
    return f"INSERT INTO public.authorship (pubkey, author) VALUES {build_author_values(authors, pubkey)}"

    
def pipeline(file_path: str):
    """Iterates through XML document, adding the elements we need to the DB"""
    conn = connect()
    cursor = conn.cursor()
    context = etree.iterparse(file_path, events=("start", "end"), dtd_validation=True)
    try:
        # guided by https://stackoverflow.com/questions/9856163/using-lxml-and-iterparse-to-parse-a-big-1gb-xml-file
        for event, elem in context:
            if event == "end" and (elem.tag == 'inproceedings' or elem.tag == 'article'):
                item_representation, authors, pubkey = handle_tag_type(elem)
                print(f'Converting {pubkey}...')
                item_insert = build_item_insert(item_representation, elem.tag)
                cursor.execute(item_insert)
                
                if len(authors) > 0:
                    authors_insert = build_authors_insert(authors, pubkey)
                    cursor.execute(authors_insert)
                
                conn.commit()
                print(f'{pubkey} comitted!')
                elem.clear()
    except Exception as err:
        print(f'Error: {err}')
    finally: 
        print('Closing connection...')
        cursor.close()
        conn.close()
        print('Connection closed')
                        
if __name__ == '__main__':
    pipeline('../materials/dblp.xml')
