from flask import Blueprint, request, current_app, jsonify, render_template

from repository.analyse_repo import AnalyseRepository
import folium
from flask import jsonify
from opencage.geocoder import OpenCageGeocode

from services.map_service import plot_avg_victims_on_map, plot_active_groups_on_map, plot_influential_groups_on_map

bp_analyse = Blueprint('bp_analyse', __name__)


@bp_analyse.route("/generate_map", methods=["POST"])
def generate_map():
    try:
        query = request.form.get("query")
        region = request.form.get("region")
        country = request.form.get("country")

        if query == "influential_groups":
            map_path = get_influential_groups(region, country)
        elif query == "active_groups":
            map_path = get_active_groups_by_region(region)
        elif query == "avg_victims":
            if region == "" or region == None:
                region = 5
            else:
                region = int(region)
            map_path = find_avg_victims_per_region(region)
        else:
            return "Invalid query", 400


        return render_template("map.html", map_path=map_path)

    except Exception as e:
        return str(e), 400

@bp_analyse.route('/find_lethal_attack', methods=['GET'])
def find_lethal_attack_type():
    max_depth = request.args.get('max_depth', 10000)

    try:
        repo = AnalyseRepository(current_app.driver)
        paths = repo.Find_most_lethal_attacktypes(max_depth)


        return jsonify(paths), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp_analyse.route('/find_avg_victims', methods=['GET'])
def find_avg_victims_per_region(region):
    try:
        repo = AnalyseRepository(current_app.driver)
        data = repo.find_avg_victims_per_region(region)
        map_html = plot_avg_victims_on_map(data)
        return map_html
    except Exception as e:
        raise e



#מציאת המטרות הכי פגיעות
@bp_analyse.route('/find_victims_per_grop', methods=['GET'])
def Find_most_victims_per_grop():
    max_depth = request.args.get('max_depth', 10000)

    try:
        repo = AnalyseRepository(current_app.driver)
        paths = repo.Find_most_victims_per_grop(max_depth)


        return jsonify(paths), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp_analyse.route('/get_active_groups', methods=['GET'])
def get_active_groups_by_region(region):
    print("get_active_groups_by_region", region)
    if region == "" or region == None:
        region = "all"
    try:
        repo = AnalyseRepository(current_app.driver)
        groups = repo.get_active_groups_by_region(region)

        map_html = plot_active_groups_on_map(region,groups)
        return map_html
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# @bp_analyse.route('/get_influential_groups', methods=['GET'])
def get_influential_groups(region,country):
    if region == "":
        region = None
    try:
        repo = AnalyseRepository(current_app.driver)
        groups = repo.get_influential_groups(region)
        print(groups)

        map_html = plot_influential_groups_on_map(region, groups)

        return map_html
    except Exception as e:
        raise e