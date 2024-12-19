from flask import Blueprint, request, current_app, jsonify

from repository.analyse_repo import AnalyseRepository

bp_analyse = Blueprint('bp_analyse', __name__)

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

#מציאת ממוצע הנפגעים פר אירוע לפי איזור
@bp_analyse.route('/find_avj', methods=['GET'])
def find_avj_victims_per_event():
    max_depth = request.args.get('max_depth', 10000)

    try:
        repo = AnalyseRepository(current_app.driver)
        paths = repo.Find_avj_victims_per_event(max_depth)


        return jsonify(paths), 200
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
    max_depth = request.args.get('region', "South America")

    try:
        repo = AnalyseRepository(current_app.driver)
        paths = repo.get_active_groups_by_region(max_depth)


        return jsonify(paths), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500