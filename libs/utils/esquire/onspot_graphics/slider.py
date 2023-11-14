import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import LinearLocator
from io import BytesIO

class SliderGraph:
    def __init__(
        self, 
        val, 
        labels=['Low','Moderate','High'], 
        bins=36
    ):
        """
        val :       A value between 0 and 1 that denotes how far along the slider the button should be positioned.
        labels :    List of x axis labels which will be evenly spaced along the axis.
        """
        val = (val * (bins-2)) + 1

        data = pd.DataFrame([np.arange(bins).T]*2)

        g = sns.heatmap(
            data=data,
            cmap='RdYlGn',
            # square=True,
            cbar=False,
        )

        g.xaxis.set_major_locator(LinearLocator(numticks=len(labels)))
        g.xaxis.set_ticklabels(labels)
        g.xaxis.set_tick_params(length=0)
        g.yaxis.set_ticks([])

        img = plt.imread('libs/azure/functions/blueprints/esquire/location_insights/assets/slider.png')
        hang = 0.3
        plt.imshow(img, extent=[val-1-hang, val+1+hang, -0-hang, 2+hang], clip_on=False, zorder=1)
        g.figure.set_size_inches(10, 0.5)

    def export(export_path=None, return_bytes=False):
        if return_bytes:
            buffer = BytesIO()
            plt.savefig(buffer, transparent=True, dpi=300, bbox_inches='tight')
            plt.close()
            buffer.seek(0)
            return buffer
        if export_path != None:
            plt.savefig(fname=export_path, transparent=True, dpi=300, bbox_inches='tight')
            plt.close()
        else:
            plt.show()