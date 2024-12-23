import os
import folium
from flask import jsonify, current_app
from opencage.geocoder import OpenCageGeocode

API_KEY = "38a6b2d679e845cebdfacedb8e58ea74"
geocoder = OpenCageGeocode(API_KEY)


regions_coordinates = {
    "Central America & Caribbean": {"latitude": 15.0, "longitude": -85.0},
    "North America": {"latitude": 40.0, "longitude": -100.0},
    "Southeast Asia": {"latitude": 10.0, "longitude": 105.0},
    "Western Europe": {"latitude": 50.0, "longitude": 5.0},
    "East Asia": {"latitude": 35.0, "longitude": 110.0},
    "South America": {"latitude": -15.0, "longitude": -60.0},
    "Eastern Europe": {"latitude": 55.0, "longitude": 25.0},
    "Sub-Saharan Africa": {"latitude": -10.0, "longitude": 20.0},
    "Middle East & North Africa": {"latitude": 25.0, "longitude": 45.0},
    "Australasia & Oceania": {"latitude": -25.0, "longitude": 135.0},
    "South Asia": {"latitude": 20.0, "longitude": 80.0},
    "Central Asia": {"latitude": 45.0, "longitude": 70.0},
}


def plot_avg_victims_on_map(data):

    m = folium.Map(location=[0, 0], zoom_start=2)  # מפה עם מבט עולמי

    for record in data:
        region = record["region"]
        print(region)
        avg_victims = record["avg_victims_per_event"]

        result = geocoder.geocode(region)
        if result and len(result) > 0:
            lat = result[0]['geometry']['lat']
            lng = result[0]['geometry']['lng']

            folium.Marker(
                location=[lat, lng],
                popup=f"Region: {region}<br>Avg Victims: {avg_victims}",
                icon=folium.Icon(color="red", icon="info-sign")
            ).add_to(m)

    m.save("avg_victims_map.html")
    return jsonify({"message": "Map created", "file": "avg_victims_map.html"})

def plot_active_groups_on_map(region, groups):

    # Create map centered globally or on a specific region
    if region.lower() != "all":
        geocode_result = geocoder.geocode(region)
        if not geocode_result:
            raise ValueError("Region coordinates not found")

        lat = geocode_result[0]['geometry']['lat']
        lng = geocode_result[0]['geometry']['lng']
        m = folium.Map(location=[lat, lng], zoom_start=5)
    else:
        m = folium.Map(location=[0, 0], zoom_start=2)

    # Add markers for each region
    for group in groups:
        region_name = group['region_name']
        top_groups = group['top_groups']

        # Geocode region for marker placement
        region_result = geocoder.geocode(region_name)
        if region_result:
            region_lat = region_result[0]['geometry']['lat']
            region_lng = region_result[0]['geometry']['lng']

            # Prepare popup content with top 5 groups
            popup_content = f"<b>Region:</b> {region_name}<br><b>Top Groups:</b><ul>"
            for top_group in top_groups:
                popup_content += f"<li>{top_group['group_name']}: {top_group['event_count']} events</li>"
            popup_content += "</ul>"

            folium.Marker(
                location=[region_lat, region_lng],
                popup=popup_content,
                icon=folium.Icon(color="blue", icon="info-sign")
            ).add_to(m)

    # Save the map to HTML
    map_path = "templates/active_groups_map.html"
    m.save(map_path)
    return map_path


def plot_influential_groups_on_map(region, country, groups):

    if region:
        landmarks = regions_coordinates.get(region)

        if landmarks:
            map_center = [landmarks["latitude"],landmarks["longitude"]]
            zoom_start = 5

            m = folium.Map(location=map_center, zoom_start=zoom_start)

            popup_content = "<b>Top Groups:</b><ul>"
            for group in groups:
                popup_content += f"<li>{group['group_name']}: {group['total_links']} links</li>"
            popup_content += "</ul>"

            folium.Marker(
                    location=[landmarks["latitude"],landmarks["longitude"]],
                    popup=popup_content,
                    icon=folium.Icon(color="blue", icon="info-sign")
            ).add_to(m)


    map_path = os.path.join(current_app.root_path, "static", "influential_groups_map.html")
    m.save(map_path)
    return "/static/influential_groups_map.html"