from flask import Blueprint, request, current_app, jsonify, render_template

from repository.analyse_repo import AnalyseRepository
import folium
from flask import jsonify
from opencage.geocoder import OpenCageGeocode

from services.map_service import plot_avg_victims_on_map, plot_active_groups_on_map, plot_influential_groups_on_map

bp_analyse = Blueprint('bp_analyse', __name__)


@bp_analyse.route("/generate_map", methods=["POST"])
def generate_map():
    query = request.form.get("query")
    region = request.form.get("region")
    country = request.form.get("country")

    if query == "influential_groups":
        # קריאה לפונקציה המתאימה
        map_path = get_influential_groups(region, country)
    elif query == "active_groups":
        # קריאה לפונקציה המתאימה
        map_path = get_active_groups_by_region(region, country)
    else:
        return "Invalid query", 400

    # הפניה לקובץ המפה
    return render_template("map.html", map_path=map_path)
#מציאת סוגי ההתקפות הקטלניות ביותר
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
def find_avg_victims_per_region():
    top_regions = request.args.get('top_regions', 5)

    try:

        repo = AnalyseRepository(current_app.driver)
        data = repo.find_avg_victims_per_region( top_regions)

        return plot_avg_victims_on_map(data), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500



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
def get_active_groups_by_region():
    region = request.args.get('region', "all")

    try:
        repo = AnalyseRepository(current_app.driver)
        groups = repo.get_active_groups_by_region(region)

        return plot_active_groups_on_map(region, groups), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# @bp_analyse.route('/get_influential_groups', methods=['GET'])
def get_influential_groups(region,country):
    # region = request.args.get('region', None)
    # country = request.args.get('country', None)

    try:
        repo = AnalyseRepository(current_app.driver)
        groups = repo.get_influential_groups(region, country)

        # Generate map with influential groups
        map_html = plot_influential_groups_on_map(region, country, groups)

        return map_html
    except Exception as e:
        return jsonify({"error": str(e)}), 500