from django.http import JsonResponse
from django.shortcuts import render
from neomodel import Traversal
from neomodel.sync_ import match
from .models import Movie, Person


def movies_index(request):
    movies = Movie.nodes.all()
    return render(request, "index.html", {"movies": movies})


def graph(request):
    nodes = []
    rels = []
    movies = Movie.nodes.has(actors=True)

    i = 0

    def ensure_node(n):
        try:
            return nodes.index(n)
        except ValueError:
            nodes.append(n)
            return len(nodes) - 1

    for movie in movies:
        mnode = {"id": movie.element_id, "title": movie.title, "label": "movie"}
        m_idx = ensure_node(mnode)

        # 1) Actores
        for person in movie.actors:
            p = {"id": person.element_id, "title": person.name, "label": "actor"}
            p_idx = ensure_node(p)
            rels.append({"source": p_idx, "target": m_idx, "type": "ACTED_IN"})

        # 2) Directores
        for person in movie.directors:
            p = {"id": person.element_id, "title": person.name, "label": "director"}
            p_idx = ensure_node(p)
            rels.append({"source": p_idx, "target": m_idx, "type": "DIRECTED"})

        # # 3) Guionistas
        # for person in movie.writers:  # <- corrige el typo aquí
        #     p = {"id": person.element_id, "title": person.name, "label": "writer"}
        #     p_idx = ensure_node(p)
        #     rels.append({"source": p_idx, "target": m_idx, "type": "WROTE"})

        # 4) Productores
        for person in movie.producers:
            p = {"id": person.element_id, "title": person.name, "label": "producer"}
            p_idx = ensure_node(p)
            rels.append({"source": p_idx, "target": m_idx, "type": "PRODUCED"})

        # 5) Reviewers (si los manejas)
        for person in movie.reviewers:
            p = {"id": person.element_id, "title": person.name, "label": "reviewer"}
            p_idx = ensure_node(p)
            rels.append({"source": p_idx, "target": m_idx, "type": "REVIEWED"})

    return JsonResponse({"nodes": nodes, "links": rels})

    return JsonResponse({"nodes": nodes, "links": rels})


def search(request):
    try:
        q = request.GET["q"]
    except KeyError:
        return JsonResponse([])

    movies = Movie.nodes.filter(title__icontains=q)
    return JsonResponse(
        [
            {
                "id": movie.element_id,
                "title": movie.title,
                "tagline": movie.tagline,
                "released": movie.released,
                "label": "movie",
            }
            for movie in movies
        ],
        safe=False,
    )


def serialize_cast(person, job, rel=None):
    return {
        "id": person.element_id,
        "name": person.name,
        "job": job,
        "role": rel.roles if rel else None,
    }


def movie_by_title(request, title):
    movie = Movie.nodes.get(title=title)
    cast = []

    for person in movie.directors:
        cast.append(serialize_cast(person, "directed"))

    for person in movie.writters:
        cast.append(serialize_cast(person, "wrote"))

    for person in movie.producers:
        cast.append(serialize_cast(person, "produced"))

    for person in movie.reviewers:
        cast.append(serialize_cast(person, "reviewed"))

    for person in movie.actors:
        rel = movie.actors.relationship(person)
        cast.append(serialize_cast(person, "acted", rel))

    return JsonResponse(
        {
            "id": movie.element_id,
            "title": movie.title,
            "tagline": movie.tagline,
            "released": movie.released,
            "label": "movie",
            "cast": cast,
        }
    )
