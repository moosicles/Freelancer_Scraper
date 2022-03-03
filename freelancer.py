from freelancersdk.session import Session
from freelancersdk.resources.projects.exceptions import \
    JobsNotFoundException, ProjectsNotFoundException
from freelancersdk.resources.projects.projects import (
    get_projects, get_project_by_id, get_jobs, search_projects
)
from freelancersdk.resources.projects.helpers import (
    create_get_projects_object, create_get_projects_project_details_object,
    create_get_projects_user_details_object, create_search_projects_filter
)
import os
import pymongo
from rich import print
import time
import datetime
from pprint import pprint



AUTH_TOKEN = "da13e903be8011b0c3131f2b3bc354f8c7d5d35f4d1a7c74f6351cdd6541f9843d06c4ffd40a830d4ba9fbaf1d532667e12a95c90d69636bb976779e60e3256a"

as_dict = lambda **kwargs: kwargs

def _print(*args, **kwargs):
    print('\r', time.strftime('%c'), *args, **kwargs)

def as_dict(**kwargs):
    return kwargs

def get_db_info(db, table):
    seen = db[table].count_documents({})
    id_start = db[table].find_one({}, sort=[("id", pymongo.DESCENDING)])
    id_start = 0 if not id_start else id_start['id']
    return seen, id_start
    
def insert(docs:list, db:str, table:str):
    def work(docs=docs, table=table):
        if docs and docs != {}:
            try:
                db[table].insert_many(docs)
            except pymongo.errors.BulkWriteError:
                for d in docs:
                    try: db[table].insert_one(d)
                    except pymongo.errors.DuplicateKeyError: pass

    if table == 'projects':
        # populate projects, bids, and user tables
        work(docs['projects'], 'projects')
        work(docs['users'].values(), 'users')
        work(list(d[0] for d in docs['selected_bids'].values()), 'selected_bids')

    else: work()


def setup():
    db = client['freelancer']
    for table in ['projects', 'selected_bids', 'users']:
        #db[table].drop()
        db[table].create_index("id", unique=True)
    return db

def populate(table:str='projects'):
    db = setup()
    _seen, id_start = get_db_info(db, table) 
    last_id = id_start
    chunk_of = 100
    
    for docs in eval(f"get_{table}_by_id({id_start}, {chunk_of})"):
        insert(docs, db, table)
        if table != 'projects':
            last_id = docs[-1]['id']
            _seen += len(docs)    
            _print(f'[magenta3]scraped {table}:', _seen, 
                    '[magenta3]start id:', id_start,
                    '[magenta3]last id:',  last_id,
                    end='\r')
        else:
            global seen
            _print(time.strftime('%C'), str(seen), end='\r')


seen = {
    'bids':0,
    'users':0,
    'projects':0
    }
def get_projects_by_id(i, limit):
    global seen
    print('initializing get_projects_by_id...')
    seen = {
        'bids':client['freelancer']['selected_bids'].count_documents({}),
        'users':client['freelancer']['users'].count_documents({}),
        'projects':client['freelancer']['projects'].count_documents({}),
    }
    _print(time.strftime('%C'), str(seen), end='\r')

    session = Session(oauth_token=AUTH_TOKEN)

    project_details=create_get_projects_project_details_object(
        full_description=True,
        jobs=True,
        qualifications=True,
        selected_bids=True,
        #location=True,
    )
    user_details=create_get_projects_user_details_object(
        basic=True,
        #profile_description=True,
        employer_reputation=True,
        #reputation_extra=True,
        responsiveness=True,
        location=True,
    )
    user_details['compact'] = True
    while True:
        
        query = create_get_projects_object(
            project_ids=list(range(i, i+limit)),
            project_details=project_details,
            user_details=user_details
        )
        try:
            p = get_projects(session, query)
        except ProjectsNotFoundException as e:
            if 'You have made too many of these requests' in str(e):
                time.sleep(60)
            else:
                print('Error message: {}'.format(e.message))
                print('Server response: {}'.format(e.error_code))
        else:
            seen['bids'] += len(p['selected_bids'])
            seen['users'] += len(p['users'])
            seen['projects'] += len(p['projects'])
            yield p
            i += limit


def get_jobs_by_id(id_start, id_end):
    session = Session(oauth_token=AUTH_TOKEN)

    get_jobs_data = {
        'job_ids': list(range(id_start, id_end)),
        'seo_details': True,
        'lang': 'en',
    }

    try:
        j = get_jobs(session, **get_jobs_data)
    except JobsNotFoundException as e:
        print('Error message: {}'.format(e.message))
        print('Server response: {}'.format(e.error_code))
        return None
    else:
        return j



if __name__== '__main__':
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    for table in ('jobs', 'projects'):
        populate('table')
    client.close()