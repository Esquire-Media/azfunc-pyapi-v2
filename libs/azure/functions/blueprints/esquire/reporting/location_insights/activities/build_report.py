from azure.durable_functions import Blueprint
from azure.storage.blob import BlobClient, ContainerClient
from io import BytesIO
from libs.utils.azure_storage import get_blob_sas
from libs.utils.pptx import add_custom_image, replace_text
from libs.utils.esquire.onspot_graphics.demographics import Demographics
from libs.utils.esquire.onspot_graphics.observations import Observations
from libs.utils.esquire.onspot_graphics.slider import SliderGraph
from pptx import Presentation
import os, pandas as pd, re

# Create a Blueprint instance for defining Azure Functions
bp = Blueprint()


# Define an activity function
@bp.activity_trigger(input_name="settings")
def activity_locationInsights_buildReport(settings: dict):

    # container for storing data and where the report will be exported
    runtime_container = ContainerClient.from_connection_string(
        os.environ[settings["runtime_container"]["conn_str"]],
        container_name=settings["runtime_container"]["container_name"],
    )
    # container that holds static assets such as PPTX templates and image files
    resources_container = ContainerClient.from_connection_string(
        os.environ[settings["resources_container"]["conn_str"]],
        container_name=settings["resources_container"]["container_name"],
    )

    template = Presentation(BytesIO(
        resources_container.download_blob(
            blob=f"templates/{settings['template']}.pptx"
        ).content_as_bytes()
    ))
    base_slide_count = len(template.slides)
    
    for location in settings["locations_data"]:
        # load location info from blob
        locationID = location["locationID"]

        # Load location info
        location_data = pd.read_csv(get_blob_sas(BlobClient.from_connection_string(
            conn_str=os.environ[settings["runtime_container"]["conn_str"]],
            container_name=settings["runtime_container"]["container_name"],
            blob_name=location["location_blob"],
        )))
        info = location_data.iloc[0].copy()
        info["Full Address"] = (
            f"{info['Address']}, {info['City']}, {info['State']} {info['Zip']}"
        )

        # load observations data from blob
        observations_data = pd.read_csv(
            get_blob_sas(
                BlobClient.from_connection_string(
                    conn_str=os.environ[settings["runtime_container"]["conn_str"]],
                    container_name=settings["runtime_container"]["container_name"],
                    blob_name=[
                        *ContainerClient.from_connection_string(
                            conn_str=os.environ[settings["runtime_container"]["conn_str"]],
                            container_name=settings["runtime_container"]["container_name"],
                        ).list_blobs(
                            name_starts_with=f"{location['observations_blob']}/"
                        )
                    ][0]["name"],
                )
            )
        )
        observations = Observations(observations_data)

        # load demographics data from blob
        demographics_data = pd.read_csv(
            get_blob_sas(
                BlobClient.from_connection_string(
                    conn_str=os.environ[settings["runtime_container"]["conn_str"]],
                    container_name=settings["runtime_container"]["container_name"],
                    blob_name=[
                        *ContainerClient.from_connection_string(
                            conn_str=os.environ[settings["runtime_container"]["conn_str"]],
                            container_name=settings["runtime_container"]["container_name"],
                        ).list_blobs(
                            name_starts_with=f"{location['demographics_blob']}/"
                        )
                    ][0]["name"],
                )
            )
        )
        demographics = Demographics(demographics_data)

    # # load template presentation
    # template = Presentation(
    #     BytesIO(
    #         resources_container.download_blob(
    #             blob=f"templates/{settings['template']}.pptx"
    #         ).content_as_bytes()
    #     )
    # )

    # Duplicate slides for this location and track new slide indices
        new_slide_indices = []
        for idx in range(base_slide_count):
            new_slide = template.slides.add_slide(template.slides[idx])
            new_slide_indices.append(len(template.slides) - 1)  # Track new slide index

        # Perform replacements only on the newly created slides
        new_slides = [template.slides[idx] for idx in new_slide_indices]
        execute_text_replacements(
            obs=observations, 
            info=info, 
            template=template, 
            slides=new_slides
            )
        execute_graphics_replacements(
            obs=observations, 
            demos=demographics, 
            template=template, 
            resources_client=resources_container, 
            settings=settings, slides=new_slides
            )
    
    # remove the blank template slides
    template = remove_slides_by_index(template, [0,1,2])

    # use a file-like object to export pptx as bytes
    output_io = BytesIO()
    template.save(output_io)
    output_io.seek(0)

    # upload bytes to blob storage
    filename = (
        re.sub(r"[^A-Za-z0-9 ]+", "", f"{info['Owner']} {info['Full Address']}")
        .replace(" ", "_")
        .replace("__", "_")
    )
    output_blob_name = f"{settings['runtime_container']['output_blob']}/{filename}.pptx"
    runtime_container.upload_blob(name=output_blob_name, data=output_io, overwrite=True)

    return output_blob_name


def execute_graphics_replacements(
    obs: Observations,
    demos: Demographics,
    template: Presentation,
    resources_client: ContainerClient,
    settings: dict,
    slides
):
    """
    Create and export the device observations graphics.
    """
    traffic_slide = slides[1]
    demos_slide = slides[2]
    # heatmap_slide = template.slides[3]
    # creative_slide = template.slides[4]

    # __TRAFFIC SLIDE__
    # foot traffic line graph
    add_custom_image(
        file=obs.foot_traffic_graph(return_bytes=True),
        slide=traffic_slide,
        placeholder=traffic_slide.shapes[4],
    )
    # time distribution graph
    add_custom_image(
        file=obs.time_distribution_graph(return_bytes=True),
        slide=traffic_slide,
        placeholder=traffic_slide.shapes[33],
    )
    # trend score slider
    add_custom_image(
        file=SliderGraph(
            val=obs.trend_score(),
            labels=["", "Decreasing", "", "Neutral", "", "Increasing", ""],
        ).export(return_bytes=True),
        slide=traffic_slide,
        placeholder=traffic_slide.shapes[31],
    )
    # stability score slider
    add_custom_image(
        file=SliderGraph(
            obs.stability_score(),
            labels=["", "Volatile", "", "Moderate", "", "Consistent", ""],
        ).export(return_bytes=True),
        slide=traffic_slide,
        placeholder=traffic_slide.shapes[30],
    )
    # recent performance slider
    add_custom_image(
        file=SliderGraph(
            val=obs.recent_score(),
            labels=["", "Below Average", "", "Average", "", "Above Average", ""],
        ).export(return_bytes=True),
        slide=traffic_slide,
        placeholder=traffic_slide.shapes[32],
    )
    # remove slider placeholders (important to do this last and in decreasing order to not mess up other shape indexes)
    traffic_slide.shapes.element.remove(traffic_slide.shapes[32].element)
    traffic_slide.shapes.element.remove(traffic_slide.shapes[31].element)
    traffic_slide.shapes.element.remove(traffic_slide.shapes[30].element)

    # __DEMOS SLIDE__
    # gender pie graph
    add_custom_image(
        file=demos.gender_graph(return_bytes=True),
        slide=demos_slide,
        placeholder=demos_slide.shapes[29],
    )
    # marital status pie graph
    add_custom_image(
        file=demos.marriage_graph(return_bytes=True),
        slide=demos_slide,
        placeholder=demos_slide.shapes[28],
    )
    # dwelling type pie graph
    add_custom_image(
        file=demos.dwelling_graph(return_bytes=True),
        slide=demos_slide,
        placeholder=demos_slide.shapes[27],
    )
    # presence of children pie graph
    add_custom_image(
        file=demos.children_graph(return_bytes=True),
        slide=demos_slide,
        placeholder=demos_slide.shapes[4],
    )
    # age bar graph
    add_custom_image(
        file=demos.age_graph(return_bytes=True),
        slide=demos_slide,
        placeholder=demos_slide.shapes[5],
    )
    # income bar graph
    add_custom_image(
        file=demos.income_graph(return_bytes=True),
        slide=demos_slide,
        placeholder=demos_slide.shapes[26],
    )

    # # --HEATMAP SLIDE--
    # # heatmap scatter plot
    # add_custom_image(
    #     file=obs.heatmap_graph(return_bytes=True),
    #     slide=heatmap_slide,
    #     placeholder=heatmap_slide.shapes[11],
    # )
    # # promotional ad image
    # blob_client = resources_client.get_blob_client(
    #     blob=f"promotions/{settings['promotionSet']}/Ad.png"
    # )
    # add_custom_image(
    #     file=BytesIO(blob_client.download_blob().content_as_bytes()),
    #     slide=heatmap_slide,
    #     placeholder=heatmap_slide.shapes[12],
    # )
    # # promotional ad hyperlink
    # blob_client = resources_client.get_blob_client(
    #     blob=f"promotions/{settings['promotionSet']}/Ad Link.txt"
    # )
    # ad = heatmap_slide.shapes[-1]
    # ad.click_action.hyperlink.address = blob_client.download_blob().content_as_text()

    # # --CREATIVE SLIDE--
    # # placeholder indexes for each of the 3 creative images
    # creative_placeholders = {0: 18, 1: 19, 2: 20}
    # for i, blob_name in enumerate(
    #     [
    #         blob.name
    #         for blob in resources_client.list_blobs(
    #             f"creatives/{settings['creativeSet']}/"
    #         )
    #     ][:3]
    # ):
    #     # creative images
    #     blob_client = resources_client.get_blob_client(blob=blob_name)
    #     if blob_client.exists():
    #         add_custom_image(
    #             file=BytesIO(blob_client.download_blob().content_as_bytes()),
    #             slide=creative_slide,
    #             placeholder=creative_slide.shapes[creative_placeholders[i]],
    #         )


def execute_text_replacements(obs: Observations, info: dict, template: Presentation, slides):
    """
    Calls functions which update all generated text in the template pptx.
    """
    # bullet points and text data
    latest_week = obs.get_latest_week()
    best_week = obs.get_best_week()
    worst_week = obs.get_worst_week()

    replaces = {
        "{{title}}": info["Owner"].upper(),
        "{{subtitle}}": info["Full Address"],
        "{{year}}": latest_week["Year"],
        "{{latest week}}": latest_week["Week"],
        "{{latest performance}}": latest_week["Performance"],
        "{{latest range}}": latest_week["Range"],
        "{{best week}}": best_week["Week"],
        "{{best performance}}": best_week["Performance"],
        "{{best range}}": best_week["Range"],
        "{{worst week}}": worst_week["Week"],
        "{{worst performance}}": worst_week["Performance"],
        "{{worst range}}": worst_week["Range"],
        "{{bullet1}}": obs.bullet_current_performance(),
        "{{bullet2}}": obs.bullet_continuous_growth(),
        "{{bullet3}}": obs.bullet_six_weeks(),
        "{{bullet4}}": obs.bullet_budget(),
    }
    # replace text in slides with generated stats and bullet points
    shapes = []
    for slide in slides:
        for shape in slide.shapes:
            shapes.append(shape)
    replace_text(replaces, shapes)

    return True

def remove_slides_by_index(prs, removal_indices):
    # Sort indices in reverse order to avoid shifting issues
    for i in sorted(removal_indices, reverse=True):
        rId = prs.slides._sldIdLst[i].rId
        prs.part.drop_rel(rId)
        del prs.slides._sldIdLst[i]

    return prs