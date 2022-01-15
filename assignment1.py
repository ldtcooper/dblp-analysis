import psycopg2
import json
import sys
from typing import Dict, Tuple, List
from xml.etree.ElementTree import iterparse, Element

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

        return conn.cursor()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        
def get_pubkey(elem: Element) -> str:
    """Returns the primary key element from a tag"""
    return elem.get('key')

def get_text(elem: Element) -> str:
    """Returns the inner text of a tag"""
    return "".join(elem.itertext())

def build_column_tuple(elem: Element) -> CellRepresentation:
    """Returns a tuple of the element's tag name and text"""
    return (elem.tag, get_text(elem))
    

def clean_single_quotes(text: str) -> str:
    """Excapes all single quotes (') in text into a double single quote('')"""
    return text.replace("'", "''")

def handle_tag_type(elem: Element) -> Tuple[RowRepresentation, AuthorshipListing, str]:
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
    row_vals = [f"'{clean_single_quotes(el[1])}'" for el in row]
    return ', '.join(row_vals)
    
def build_item_insert(row: RowRepresentation, table_name: str) -> str:
    """Turns a row representation into a SQL query for a given table name"""
    # table names are capitalized
    table_name = table_name.capitalize()
    return f"INSERT INTO public.{table_name} ({build_row_names(row)}) VALUES({build_row_values(row)});"
    
def build_author_values(authors: AuthorshipListing, pubkey: str) -> str:
    """"Turns list of authors into values section for multiple insert"""
    keyed_authors = [f"('{pubkey}', '{clean_single_quotes(a)}')" for a in authors]
    return ', '.join(keyed_authors)

def build_authors_insert(authors: AuthorshipListing, pubkey: str) -> str:
    """Turns a list of authors into a SQL query to insert them into authorship table"""
    # INSERT INTO products(product_no, name, price) VALUES(1, 'Cheese', 9.99),(2, 'Bread', 1.99),(3, 'Milk', 2.99)
    return f"INSERT INTO public.Authorship (pubkey, author) VALUES{build_author_values(authors, pubkey)}"

    
def pipeline(file_path: str):
    """Iterates through XML document, adding the elements we need to the DB"""
    cursor = connect()
    ip = iterparse(file_path, events=("start",))
    _, root = next(ip)
    for elem in root:
        if elem.tag == 'inproceedings' or elem.tag == 'article':
            item_representation, authors, pubkey = handle_tag_type(elem)
            item_insert = build_item_insert(item_representation, elem.tag)
            authors_insert = build_authors_insert(authors, pubkey)
            cursor.execute(item_insert)
            cursor.execute(authors_insert)
            elem.clear()
                        
if __name__ == '__main__':
    pipeline('./sample.xml')
