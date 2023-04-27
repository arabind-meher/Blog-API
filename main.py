import os
from time import sleep
from typing import Optional
from datetime import datetime

from pandas import DataFrame
from fastapi import FastAPI, HTTPException
from firebase_admin import initialize_app, credentials, db
from firebase_admin.exceptions import FirebaseError
from dotenv import load_dotenv


# loading credentials to python environment
load_dotenv('credentials.env')

# creating fastapi application
app = FastAPI()

# creating connection with firebase realtime database
cred = credentials.Certificate(os.environ.get('FIREBASE_PRIVATE_KEY'))
firebase = initialize_app(
    cred, {'databaseURL': os.environ.get('DATABASE_URL')}
)

# refenencing database path
blogs_ref = db.reference('/blogs')


@app.get('/blogs')
async def read_all_blogs():
    blogs = blogs_ref.get()

    if blogs is None:
        return HTTPException(status_code=404, detail='Blog Not Found')

    blogs_df = DataFrame(
        data=blogs.values(), index=blogs.keys()
    ).sort_values(
        by=['timestamp'], ascending=False
    ).reset_index()

    return blogs_df.to_dict(orient='records')


# returning all blogs
@app.get('/blogs/{blog_id}')
async def read_blog(blog_id: str):
    blog = blogs_ref.child(blog_id).get()

    if blog is None:
        return HTTPException(status_code=404, detail='Blog Not Found')

    return blog


# returning blog with given id
@app.post('/blogs/add')
async def create_blog(title: str, body: str, tags: str):
    content = {
        'timestamp': datetime.timestamp(datetime.now()),
        'title': title.strip(),
        'body': body.strip(),
        'tags': list(map(lambda tag: tag.strip(), tags.split(',')))
    }

    try:
        blogs_ref.push(content)
    except FirebaseError:
        return HTTPException(status_code=500, detail=FirebaseError)

    return HTTPException(status_code=201, detail='Blog Created')


# updaing blog with given id
@app.put('/blogs/update/{blog_id}')
async def update_blog(blog_id: str, title: Optional[str] = None, body: Optional[str] = None, tags: Optional[str] = None):
    blog = blogs_ref.child(blog_id).get()

    if blog is None:
        return HTTPException(status_code=404, detail='Blog Not Found')

    if title is None:
        title = blog.get('title')
    if body is None:
        body = blog.get('body')
    if tags is None:
        tags = ','.join(blog.get('tags'))

    try:
        blogs_ref.child(blog_id).update({
            'title': title.strip(),
            'body': body.strip(),
            'tags': list(map(lambda tag: tag.strip(), tags.split(',')))
        })
    except FirebaseError:
        return HTTPException(status_code=500, detail=FirebaseError)

    return HTTPException(status_code=202, detail='Blog Updated')


# deleting blog with given id
@app.delete('/blog/delete/{blog}')
async def delete_blog(blog_id: str):
    if blogs_ref.child(blog_id).get() is None:
        return HTTPException(status_code=404, detail='Blog Not Found')

    try:
        blogs_ref.child(blog_id).delete()
    except FirebaseError:
        return HTTPException(status_code=500, detail=FirebaseError)

    return HTTPException(status_code=202, detail='Blog Deleted')
