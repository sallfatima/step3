import json
import geopandas as gpd

def generate_tfvars_from_geojson(geojson_path: str, zone: str, area_name: str, feature: str, output_tfvars: str):
    gdf = gpd.read_file(geojson_path)

    with open(output_tfvars, "w") as f:
        f.write("cli_args_per_job = {\n")
        for idx, row in gdf.iterrows():
            coords = [[round(x, 15), round(y, 15)] for x, y in row.geometry.exterior.coords]
            job_name = f"{zone}25-4{idx + 1}"

            # Construire le string polygon sans espace
            coord_str = "[" + ",".join([f"[{x},{y}]" for x, y in coords]) + "]"

            f.write(f'  "{job_name}" = [')
            f.write(f'"area.name=africa/nigeria/{zone}/{area_name}{idx + 1}", ')
            f.write(f'"inference=[{feature}]", ')
            f.write(f'"area.polygon={coord_str}", ')
            f.write(f'"annotations_filename=annotations.json", ')
            f.write("],\n")
        f.write("}\n")

    print(f"✅ Fichier tfvars généré : {output_tfvars}")


# Exemple d'appel
if __name__ == "__main__":
    #generate_tfvars_from_geojson("abuja_custom.geojson", "abuja","abuja_custom", "build,card,retrieve", "abuja_cli_args_per_job.json")
    #generate_tfvars_from_geojson("abuja_custom26.geojson", "abuja","abuja_custom26_", "predict", "abuja26_inf_cli_args_per_job.json")
    #  generate_tfvars_from_geojson("abuja_custom19_9.geojson", "abuja","abuja_custom19_9", "predict", "abuja19_9_inf_cli_args_per_job.json")
    # generate_tfvars_from_geojson("abuja_custom19_10.geojson", "abuja","abuja_custom19_10", "predict", "abuja19_10_inf_cli_args_per_job.json")
    # generate_tfvars_from_geojson("abuja_custom19_11.geojson", "abuja","abuja_custom19_11", "predict", "abuja19_11_inf_cli_args_per_job.json")
   generate_tfvars_from_geojson("abuja_custom25_4.geojson", "abuja","abuja_custom25_4", "predict", "abuja25_4_inf_cli_args_per_job.json")
  