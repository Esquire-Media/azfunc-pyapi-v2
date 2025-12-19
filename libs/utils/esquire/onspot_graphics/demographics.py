import numpy as np
import matplotlib
matplotlib.use('Agg')
from matplotlib import pyplot as plt
import seaborn as sns
from .crosstab_jointplot import crosstab_jointplot
from io import BytesIO
from pathlib import Path

roboto_reg = Path("libs/utils/esquire/onspot_graphics/fonts/Roboto-Regular.ttf")
roboto_bold = Path("libs/utils/esquire/onspot_graphics/fonts/Roboto-Bold.ttf")
roboto_black = Path("libs/utils/esquire/onspot_graphics/fonts/Roboto-Black.ttf")

class Demographics:
    def __init__(self, data):
        """
        Object representing a demographic audience.

        Params:

        * data : pandas dataframe of a raw Onspot Complete Demographics file.
        """

        # get a single categorical value for each demographic dummy column (that we care about saving)
        self.df = data.copy()
        self.df["income"] = self.df.apply(get_income_group, axis=1)
        self.df["age"] = self.df.apply(get_age_group, axis=1)
        self.df["education"] = self.df.apply(get_education_level, axis=1)
        self.df["gender"] = self.df.apply(
            lambda x: "male"
            if x["male"] == 1
            else "female"
            if x["female"] == 1
            else "NA",
            axis=1,
        )
        self.df["dwelling"] = self.df.apply(
            lambda x: "single"
            if x["dwelling_type single family"] == 1
            else "multi"
            if x["dwelling_type apt/multi-family"] == 1
            else "NA",
            axis=1,
        )
        self.df["marital_status"] = self.df.apply(
            lambda x: "married"
            if x["married"] == 1
            else "unmarried"
            if x["married"] == 0
            else "NA",
            axis=1,
        )
        self.df["children"] = self.df.apply(
            lambda x: "has_children"
            if x["presence_of_children"] > 0
            else "no_children",
            # if x["presence_of_children"] == 0
            # else "NA",
            axis=1,
        )
        self.df["veteran"] = self.df.apply(
            lambda x: "has_veteran" if x["presence_of_veteran"] == 1 else "no_veteran",
            axis=1,
        )

        # drop unneeded columns
        self.df = self.df[
            [
                "location (venue)",
                "state",
                "city",
                "zip",
                "zip4",
                "hashed device id",
                "number of instances (count seen at location)",
                "income",
                "age",
                "gender",
                "dwelling",
                "marital_status",
                "children",
                "veteran",
            ]
        ]

    def age_graph(self, title="Age Distribution", export_path=None, return_bytes=False):
        """
        Returns a barplot of the age group distribution.

        Parameters:

            title : str (Default='Age Distribution')
                 Graph title.
            export_path : str (Default=None)
                If set, the graph will be exported as a PNG image file to the specified location.
        """
        # define the sort order
        age_sorter = [
            "18-24",
            "25-29",
            "30-34",
            "35-39",
            "40-44",
            "45-49",
            "50-54",
            "55-59",
            "60-64",
            "65-69",
            "70+",
        ]
        age_sorter_idx = [x for x in age_sorter if x in self.df["age"].unique()]
        age_df = self.df.set_index("age").loc[age_sorter_idx]

        # plot age distribution
        plt.figure(figsize=(7.5, 4), facecolor="w")
        g = sns.countplot(data=age_df, x=age_df.index, color="#AF0C0F", order=age_sorter)

        plt.ylabel("")
        plt.xlabel("Age")
        plt.title(title, fontsize=14, font=roboto_bold)
        plt.yticks([])
        plt.tight_layout()

        if age_df.empty:
            plt.ylabel("")
            plt.xlabel("")
            plt.yticks([])
            plt.xticks([])
            g.text(
                0.5,
                0.5,
                'Insufficient Data',
                bbox={'facecolor':'white','alpha':1,'edgecolor':'none','pad':1},
                ha='center', 
                va='center',
                fontdict={
                    'font':roboto_bold,
                    'fontsize':30
                }) 

        # export if specified
        if return_bytes:
            buffer = BytesIO()
            plt.savefig(buffer)
            plt.close()
            buffer.seek(0)
            return buffer
        if export_path != None:
            plt.savefig(fname=export_path)
            plt.close()
        else:
            plt.show()

    def income_graph(self, title="Income Distribution", export_path=None, return_bytes=False):
        """
        Returns a barplot of the income group distribution.

        Parameters:

            title : str (Default='Income Distribution')
                 Graph title.
            export_path : str (Default=None)
                If set, the graph will be exported as a PNG image file to the specified location.
        """
        # define the sort order
        inc_sorter = ["0-25k", "25-50k", "50-75k", "75-100k", "100k+"]
        inc_sorter_idx = [x for x in inc_sorter if x in self.df["income"].unique()]
        inc_df = self.df.set_index("income").loc[inc_sorter_idx]

        # plot age distribution
        plt.figure(figsize=(7.5, 4), facecolor="w")
        g = sns.countplot(data=inc_df, x=inc_df.index, color="#478EE2", order=inc_sorter)

        plt.ylabel("")
        plt.xlabel("Income")
        plt.title(title, fontsize=14, font=roboto_bold)
        plt.yticks([])
        plt.tight_layout()

        if inc_df.empty:
            plt.ylabel("")
            plt.xlabel("")
            plt.yticks([])
            plt.xticks([])
            g.text(
                0.5,
                0.5,
                'Insufficient Data',
                bbox={'facecolor':'white','alpha':1,'edgecolor':'none','pad':1},
                ha='center', 
                va='center',
                fontdict={
                    'font':roboto_bold,
                    'fontsize':30
                }) 

        # export if specified
        if return_bytes:
            buffer = BytesIO()
            plt.savefig(buffer)
            plt.close()
            buffer.seek(0)
            return buffer
        if export_path != None:
            plt.savefig(fname=export_path)
            plt.close()
        else:
            plt.show()
    def gender_graph(self, title="Gender", export_path=None, return_bytes=False):
        """
        Returns a donut plot of the gender distribution.

        Parameters:

            title : str (Default='Gender')
                 Graph title.
            export_path : str (Default=None)
                If set, the graph will be exported as a PNG image file to the specified location.
        """
        # gender graph
        labels = ["male", "female"]
        vc = self.df["gender"].value_counts()
        for label in labels:
            if label not in vc.index:
                vc[label] = 0
        vc = vc[labels]
        pcts = 100 * vc / vc.sum()

        return donut_graph(
            groups=pcts.values,
            colors=["#478EE2", "#FA6164"],
            names=["Male", "Female"],
            title=title,
            export_path=export_path,
            return_bytes=return_bytes
        )

    def dwelling_graph(self, title="Dwelling Type", export_path=None, return_bytes=False):
        """
        Returns a donut plot of the dwelling type distribution.

        Parameters:

            title : str (Default='Dwelling Type')
                 Graph title.
            export_path : str (Default=None)
                If set, the graph will be exported as a PNG image file to the specified location.
        """
        # dwelling graph
        labels = ["single", "multi"]
        vc = self.df["dwelling"].value_counts()
        for label in labels:
            if label not in vc.index:
                vc[label] = 0
        vc = vc[labels]
        pcts = 100 * vc / vc.sum()

        return donut_graph(
            groups=pcts.values,
            colors=["#FEB131", "#478EE2"],
            names=["SingleFamily", "MultiFamily"],
            title=title,
            export_path=export_path,
            return_bytes=return_bytes
        )

    def marriage_graph(self, title="Marital Status", export_path=None, return_bytes=False):
        """
        Returns a donut plot of the marital status distribution.

        Parameters:

            title : str (Default='Marital Status')
                 Graph title.
            export_path : str (Default=None)
                If set, the graph will be exported as a PNG image file to the specified location.
        """
        # marital status graph
        labels = ["married", "unmarried"]
        vc = self.df["marital_status"].value_counts()
        for label in labels:
            if label not in vc.index:
                vc[label] = 0
        vc = vc[labels]
        pcts = 100 * vc / vc.sum()

        return donut_graph(
            groups=pcts.values,
            colors=["#FA6164", "#378839"],
            names=["Married", "Unmarried"],
            title=title,
            export_path=export_path,
            return_bytes=return_bytes
        )

    def children_graph(self, title="Presence of Children", export_path=None, return_bytes=False):
        """
        Returns a donut plot of the presence of children distribution.

        Parameters:

            title : str (Default='Presence of Children')
                 Graph title.
            export_path : str (Default=None)
                If set, the graph will be exported as a PNG image file to the specified location.
        """
        # presence of children graph
        labels = ["has_children", "no_children"]
        vc = self.df["children"].value_counts()
        for label in labels:
            if label not in vc.index:
                vc[label] = 0
        vc = vc[labels]
        pcts = 100 * vc / vc.sum()

        return donut_graph(
            groups=pcts.values,
            colors=["#6549DA", "#6CB0F2"],
            names=["Children in Home", "No Children"],
            title=title,
            export_path=export_path,
            return_bytes=return_bytes
        )

    def veteran_graph(self, title="Veteran Status", export_path=None, return_bytes=False):
        """
        Returns a donut plot of the veteran status distribution.

        Parameters:

            title : str (Default='Veteran Status')
                 Graph title.
            export_path : str (Default=None)
                If set, the graph will be exported as a PNG image file to the specified location.
        """
        # presence of veteran graph
        labels = ["no_veteran", "has_veteran"]
        vc = self.df["veteran"].value_counts()
        for label in labels:
            if label not in vc.index:
                vc[label] = 0
        vc = vc[labels]
        pcts = 100 * vc / vc.sum()

        return donut_graph(
            groups=pcts.values,
            colors=["#6549DA", "#6CB0F2"],
            names=["Not Veteran", "Veteran"],
            title=title,
            export_path=export_path,
            return_bytes=return_bytes
        )

    def age_income_crosstabs(self, export_path=None, return_bytes=False):
        """
        Returns a 2D heatmap jointplot of the crosstabs between age and income.
        """
        age_sorter = ["70+","65-69","60-64","55-59","50-54","45-49","40-44","35-39","30-34","25-29","18-24"]
        income_sorter = ["0-25k", "25-50k", "50-75k", "75-100k", "100k+"]

        return crosstab_jointplot(
            df=self.df[(self.df["age"] != "NA") & (self.df["income"] != "NA")],
            x_col="income",
            y_col="age",
            x_order=income_sorter,
            y_order=age_sorter,
            aspect_ratio=0.75,
            heatmap_palette="Purples",
            ybar_color="#221473",
            xbar_color="#221473",
            export_path=export_path,
            return_bytes=return_bytes
        )

### MISCELLANEOUS ULTILTY FUNCTIONS ###


def get_income_group(x):
    """
    Returns the income group based on a device demographics row.
    """
    if (
        x["household_income under $15000"] > 0
        or x["household_income $15000 - $24999"] > 0
    ):
        return "0-25k"
    elif (
        x["household_income $25000 - $34999"] > 0
        or x["household_income $35000 - $49999"] > 0
    ):
        return "25-50k"
    elif x["household_income $50000 - $74999"] > 0:
        return "50-75k"
    elif x["household_income $75000 - $99999"] > 0:
        return "75-100k"
    elif (
        x["household_income $100000 - $149999"] > 0
        or x["household_income $150000 - $199999"] > 0
        or x["household_income $200000 - $249999"] > 0
        or x["household_income $250000+"] > 0
    ):
        return "100k+"
    else:
        return "NA"


def get_age_group(x):
    """
    Returns the age group based on a device demographics row.
    """
    val = "NA"
    for col in x.index[x.index.str.contains("estimated_age")]:
        if x[col] == 1:
            val = col.split(" ")[1]
    return val


def get_education_level(x):
    """
    Returns the education level based on a device demographics row.
    """
    val = "NA"
    if x["completed high school"] == 1 or x["some college"] == 1:
        val = "no_college_degree"
    elif (
        x["completed college"] == 1
        or x["attended vocational/technical school"]
        or x["completed graduate school"] == 1
    ):
        val = "college_degree"
    return val


def donut_graph(groups, colors, names, title, export_path, return_bytes=False):
    """
    Utility function for creating a donut graph based on a list of categories and their corresponding values.

    Parameters:
        groups : list
            A list of numerical values corresponding to the donut graph percentages.
        colors : list
            A list of colors to map onto the group values.
        names : list
            A list of names to map onto the group values.
        title : str
            Graph title.
        export_path : str
            If set, the graph will be exported as a PNG image file to the specified location.
    """
    if all(np.isnan(val) for val in groups):
        return null_donut(groups, colors, names, title, export_path, return_bytes)

    fig = plt.figure(figsize=(4, 4), facecolor="w")
    graph = plt.pie(groups, startangle=90)

    for i in range(len(colors)):
        graph[0][i].set_color(colors[i])

    my_circle = plt.Circle((0, 0), 0.6, color="white")

    p = plt.gcf()
    p.gca().add_artist(my_circle)

    # percentage 1
    plt.text(
        0,
        0.15,
        str(round(groups[0], 1)) + "%",
        fontsize=25,
        font=roboto_black,
        horizontalalignment="center",
        color=colors[0],
    )

    # percentage 2
    plt.text(
        0,
        -0.25,
        str(round(groups[1], 1)) + "%",
        fontsize=25,
        font=roboto_black,
        horizontalalignment="center",
        color=colors[1],
    )

    # main title
    plt.text(
        0,
        1.1,
        title,
        fontsize=40 - 4 * np.sqrt(len(title)),
        font=roboto_bold,
        horizontalalignment="center",
        color="black",
    )

    # name 1
    plt.text(
        -0.1,
        -1.2,
        names[0],
        fontsize=30 - 4 * np.sqrt(len(title)),
        font=roboto_bold,
        horizontalalignment="right",
        color=colors[0],
    )
    # vs
    plt.text(0, -1.2, "vs", fontsize=12, font=roboto_reg, horizontalalignment="center")
    # name 2
    plt.text(
        0.1,
        -1.2,
        names[1],
        fontsize=30 - 4 * np.sqrt(len(title)),
        font=roboto_bold,
        horizontalalignment="left",
        color=colors[1],
    )

    # export if specified
    if return_bytes:
        buffer = BytesIO()
        plt.savefig(buffer)
        plt.close()
        buffer.seek(0)
        return buffer
    if export_path != None:
        plt.savefig(fname=export_path)
        plt.close()
    else:
        plt.show()


def null_donut(groups, colors, names, title, export_path, return_bytes=False):
    """
    Utility function for creating a donut graph based on a list of categories and their corresponding values.

    Parameters:
        groups : list
            A list of numerical values corresponding to the donut graph percentages.
        colors : list
            A list of colors to map onto the group values.
        names : list
            A list of names to map onto the group values.
        title : str
            Graph title.
        export_path : str
            If set, the graph will be exported as a PNG image file to the specified location.
    """

    groups = [0,1]
    print(groups)
    fig = plt.figure(figsize=(4, 4), facecolor="w")
    graph = plt.pie(groups, startangle=90)

    for i in range(len(colors)):
        graph[0][i].set_color('grey')

    my_circle = plt.Circle((0, 0), 0.9, color="white")

    p = plt.gcf()
    p.gca().add_artist(my_circle)

    # percentage 1
    plt.text(
        0,
        -0.3,
        "Insufficient\nData",
        fontsize=25,
        font=roboto_black,
        horizontalalignment="center",
        color='k',
    )

    # main title
    plt.text(
        0,
        1.1,
        title,
        fontsize=40 - 4 * np.sqrt(len(title)),
        font=roboto_bold,
        horizontalalignment="center",
        color="black",
    )

    # export if specified
    if return_bytes:
        buffer = BytesIO()
        plt.savefig(buffer)
        plt.close()
        buffer.seek(0)
        return buffer
    if export_path != None:
        plt.savefig(fname=export_path)
        plt.close()
    else:
        plt.show()