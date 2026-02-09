from datetime import datetime as dt, timedelta
import seaborn as sns
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import matplotlib.dates as mdates
from matplotlib.ticker import MultipleLocator
from matplotlib.markers import MarkerStyle
import plotly.express as px
from io import BytesIO
import os
import math
import numpy as np
from libs.utils.time import get_local_timezone
from libs.azure.key_vault import KeyVaultClient

class Observations:
    def __init__(self, data):
        """
        Object representing a device observations audience.

        Params:

        * data : pandas dataframe of a raw Onspot Device Observations file.
        """
        
        # determine the timezone where this location exists
        # local_timezone = get_local_timezone(latitude=data['lat'].median(), longitude=data['lng'].median())
        # data['Datetime'] = data['timestamp'].apply(lambda x: dt.fromtimestamp(x/1000, tz=local_timezone))
        # data["Datetime"] = pd.to_datetime(data["Date"] + " " + data["Time"], format="ISO8601", dayfirst=False)
        dt_str = data["Date"].astype(str).str.strip() + " " + data["Time"].astype(str).str.strip()
        data["Datetime"] = _parse_iso_datetime_utc(dt_str)
        
        # get date and time from the timestamp
        data['Date'] = data['Datetime'].apply(lambda x: x.date())
        data['Time'] = data['Datetime'].apply(lambda x: x.time())
        self.raw_data = data.copy()

        data = data.drop_duplicates(subset=['deviceid','Date'])

        # get other dates in week
        base = pd.to_datetime(data['Date'], errors='coerce', utc=True)

        # normalize to midnight, strip tz
        base = base.dt.tz_convert(None).dt.normalize()

        data['Week'] = base.dt.isocalendar().week

        data['EarliestDate'] = base - pd.to_timedelta(base.dt.weekday, unit='D')
        data['LatestDate'] = data['EarliestDate'] + pd.to_timedelta(6, unit='D')
        data['RefDate'] = data['EarliestDate'] + pd.to_timedelta(3, unit='D')
        # enforce datetime format
        data['EarliestDate'] = pd.to_datetime(data['EarliestDate'])
        data['LatestDate'] = pd.to_datetime(data['LatestDate'])
        data['RefDate'] = pd.to_datetime(data['RefDate'])

        # format as a weekly count (used for most of the summary stats)
        self.obs = data.pivot_table(
            index=['Week','RefDate','EarliestDate','LatestDate'],
            values=['deviceid'],
            aggfunc='count'
        )
        self.obs = self.obs.sort_values('RefDate').reset_index()
        self.obs['traffic_pct'] = round(100 * (self.obs['deviceid'] - self.obs['deviceid'].mean()) / self.obs['deviceid'].mean(),1)
        # calculate weeks of consecutive growth (used in summary bullet points)
        self.obs['Weeks of Growth'] = 0
        for index, row in self.obs.iterrows():
            if index > 0:
                if self.obs.loc[index]['traffic_pct'] > self.obs.loc[index-1]['traffic_pct']:
                    self.obs.at[index, 'Weeks of Growth'] = self.obs.loc[index-1]['Weeks of Growth'] + 1

        # save with latlongs retained (for heatmap)
        self.latlongs = data

    def get_latest_week(self):
        """
        Returns a dictionary with the week number, performance, and range of the most recent week.
        """
        # row with week data
        week = self.obs.iloc[-1]

        return {
            'Week'          : week['Week'],
            'Year'          : week['EarliestDate'].year,
            'RefDate'       : week['RefDate'],
            'Performance'   : plus_padding(round(week['traffic_pct'])),
            'traffic_pct'   : week['traffic_pct'],
            'Range'         : dates_to_range(
                week['EarliestDate'], 
                week['LatestDate']
            )
        }

    def get_best_week(self):
        """
        Returns a dictionary with the week number, performance, and range of the highest-traffic week.
        """
        # row with week data
        week = self.obs.iloc[self.obs['traffic_pct'].idxmax()]
        return {
            'Week'          : week['Week'],
            'RefDate'       : week['RefDate'],
            'Performance'   : plus_padding(round(week['traffic_pct'])),
            'traffic_pct'   : week['traffic_pct'],
            'Range'         : dates_to_range(
                week['EarliestDate'], 
                week['LatestDate']
            )
        }

    def get_worst_week(self):
        """
        Returns a dictionary with the week number, performance, and range of the lowest-traffic week.
        """
        # row with week data
        week = self.obs.iloc[self.obs['traffic_pct'].idxmin()]
        return {
            'Week'          : week['Week'],
            'RefDate'       : week['RefDate'],
            'Performance'   : plus_padding(round(week['traffic_pct'])),
            'traffic_pct'   : week['traffic_pct'],
            'Range'         : dates_to_range(
                week['EarliestDate'], 
                week['LatestDate']
            )
        }

    def foot_traffic_graph(self, export_path=None, return_bytes=False):
        """
        Line graph of weekly foot traffic over N weeks with ticks at monthly reference points.
        """

        fig = plt.figure(figsize=(7.5,3))
        ax = plt.subplot(111)

        self.obs["x"] = self.obs.index  # 0..N-1

        # invisible plot to get x-axis with week numbers
        g = sns.lineplot(data=self.obs, x='x', y='traffic_pct', visible=False, ax=ax)
        ax.axhline(y=0, xmin=0, xmax=1, color='gray', linestyle='--')
        ax.set_xlabel('Week')
        ax.set_ylabel('')
        ax.xaxis.set_major_locator(MultipleLocator(1)) # ensure every week number is showm

        # bottom ticks: one per week, labeled by date
        ax.set_xticks(self.obs["x"])
        ax.set_xticklabels(self.obs["RefDate"].dt.strftime("%b %d"), rotation=45, ha="right", fontsize=9)
        ax.set_xlabel("Week")
        ax.set_ylabel("")

        # invvisible plot for 1st-of-month ticks on upper x-axis
        ax2 = ax.twiny()
        sns.lineplot(
            x=self.obs['RefDate'],
            y=self.obs['traffic_pct'],
            data=self.obs,
            visible=False
        )
        ax2.xaxis.set_major_locator(mdates.DayLocator(bymonthday=[1]))
        ax2.set_xticklabels('')
        ax2.set_xlabel('')

        # # visible plot and month names (centered on 15th of each month) on upper x-axis
        ax3 = ax.twiny()
        sns.lineplot(data=self.obs, x='RefDate', y='traffic_pct', linewidth=5, color='#449fd8')
        ax3.xaxis.set_major_locator(mdates.DayLocator(bymonthday=[15]))
        ax3.xaxis.set_major_formatter(mdates.DateFormatter('%B'))
        ax3.xaxis.set_ticks_position('none')
        ax3.set_xlabel('')

        # blue marker outlines for best, worst, and latest weeks
        ax3.plot([self.get_latest_week()['RefDate']], [self.get_latest_week()['traffic_pct']], 'o', markersize=12, color='#449fd8')
        ax3.plot([self.get_best_week()['RefDate']],   [self.get_best_week()['traffic_pct']],   'o', markersize=12, color='#449fd8')
        ax3.plot([self.get_worst_week()['RefDate']],  [self.get_worst_week()['traffic_pct']],  'o', markersize=12, color='#449fd8')
        # half-fill the circles if two attributes overlap
        best_fill = 'right' if self.get_latest_week()['Week'] == self.get_best_week()['Week'] else 'full'
        worst_fill = 'right' if self.get_latest_week()['Week'] == self.get_worst_week()['Week'] else 'full'
        # colored marker centers for best, worst, and latest weeks
        ax3.plot([self.get_latest_week()['RefDate']], [self.get_latest_week()['traffic_pct']], marker=MarkerStyle('o', fillstyle='full'),     markersize=7, color='#035FC5', markeredgewidth=0)
        ax3.plot([self.get_best_week()['RefDate']],   [self.get_best_week()['traffic_pct']],   marker=MarkerStyle('o', fillstyle=best_fill),  markersize=7, color='#6CBE4F', markeredgewidth=0)
        ax3.plot([self.get_worst_week()['RefDate']],  [self.get_worst_week()['traffic_pct']],  marker=MarkerStyle('o', fillstyle=worst_fill), markersize=7, color='#E41226', markeredgewidth=0)

        # y-axis formatting
        bounds = max(abs(self.obs['traffic_pct'].min()), abs(self.obs['traffic_pct'].max())) # make sure 0 is in the center of the y-axis
        plt.ylim(-bounds*1.1, bounds*1.1)
        g.yaxis.set_major_formatter(mtick.FuncFormatter(ytick_formatter))

        plt.tight_layout()
        if return_bytes:
            buffer = BytesIO()
            fig.savefig(buffer, facecolor='w')
            plt.close()
            buffer.seek(0)
            return buffer
        if export_path != None:
            plt.savefig(fname=export_path, facecolor='white')
            plt.close()
        else:
            plt.show()

    def heatmap_graph(self, export_path=None, return_bytes=False):
        """
        Scatter plot of device latlongs.
        """

        # get range of lats and lngs
        lat_min = self.latlongs['lat'].min()
        lat_max = self.latlongs['lat'].max()
        lng_min = self.latlongs['lng'].min()
        lng_max = self.latlongs['lng'].max()
        lat_bound = abs(lat_max - lat_min)
        lng_bound = abs(lng_max - lng_min)
        
        # lat lng centerpoint
        lat_mid = lat_min + lat_bound / 2
        lng_mid = lng_min + lng_bound / 2
        # convert to km
        lat_bound *= 111
        lng_bound *= 111
        # calculate zoom level
        max_bound = abs(max(lat_bound, lng_bound * 1.4))
        zoom = 14 - math.log(max_bound)
        # calculate opacity
        opacity = 0.1 if self.obs['deviceid'].sum() > 2500 else 0.2

        # map
        fig = px.scatter_mapbox(
            self.latlongs, 
            lat="lat", 
            lon="lng", 
            opacity=opacity,
            size_max=1,
            color_discrete_sequence=["#F15A29"],
            zoom = zoom,
            center = {'lat':lat_mid,'lon':lng_mid}
        )
        # properties
        fig.update_layout(
            margin = dict(l = 0, r = 0, t = 0, b = 0),
            width=579,
            height=414,
        )
        # use custom mapbox style if mapbox token env variable is set
        if os.environ.get('mapbox_token'):
            mapbox_token = os.environ.get('mapbox_token')
        else:
            # otherwise connect to key vault
            mapbox_vault = KeyVaultClient('mapbox-service')
            mapbox_token = mapbox_vault.get_secret('mapbox-token').value
        # use mapbox token to set custom style
        fig.update_layout(
            mapbox_style="mapbox://styles/esqtech/cl8nh2452002p15logaud46pv",
            mapbox_accesstoken = mapbox_token,
        )
        # export as specified
        if return_bytes:
            buffer = BytesIO()
            fig.write_image(buffer)
            plt.close()
            buffer.seek(0)
            return buffer
        if export_path != None:
            plt.savefig(fname=export_path)
            plt.close()
        else:
            fig.show()

    def time_distribution_graph(self, export_path=None, return_bytes=False):
        """
        Returns a grid heatmap showing both time of day & day of week traffic distributions.
        """

        # utility dictionaries for ordering axes
        hours_of_day = {0:'Mid',1:'1AM',2:'2AM',3:'3AM',4:'4AM',5:'5AM',6:'6AM',7:'7AM',8:'8AM',9:'9AM',10:'10AM',11:'11AM',12:'Noon',13:'1PM',14:'2PM',15:'3PM',16:'4PM',17:'5PM',18:'6PM',19:'7PM',20:'8PM',21:'9PM',22:'10PM',23:'11PM'}
        days_of_week = {0:'Sun',1:'Mon',2:'Tue',3:'Wed',4:'Thu',5:'Fri',6:'Sat'}

        obs = self.raw_data.copy()
        # get sortable numerics for Time and Day
        obs['Hour Number'] = obs['Time'].apply(lambda x: x.hour).astype(int)
        obs['Day Number'] = obs['Date'].apply(lambda x: dt.strftime(x,'%w')).astype(int)

        # don't double-count a single observation in the same hour
        obs = obs.drop_duplicates(subset=['deviceid','Hour Number','Date'])

        # get crosstabs, normalized so 1 is the "average" cell value
        crosstab = round(pd.crosstab(obs['Day Number'], obs['Hour Number'], normalize=True) * 168,3) # 168 is the number of buckets (24 x 7)
        crosstab.columns.name = 'Hour Number'
        crosstab = crosstab.sort_values('Day Number', axis=0, key=lambda x: sort_by_list(crosstab.index, list(days_of_week.values())))

        # stacked format needed for the jointplot
        stack = crosstab.stack().reset_index().rename(columns={0:'weight'})

        # JOINTPLOT AND HEATMAP COMBINED GRAPH
        # graphing params
        nx = crosstab.shape[1]
        ny = crosstab.shape[0]
        x = 'Hour Number'
        y = 'Day Number'
        weight = 'weight'
        xtick_labels = [hours_of_day[hour_num] for hour_num in sorted(obs['Hour Number'].unique())]
        ytick_labels = [days_of_week[day_num] for day_num in sorted(obs['Day Number'].unique())]
        xtick_rotation = 90
        ytick_rotation = 0

        # skeleton for the totals bars
        g = sns.jointplot(data=stack, x=x, y=y, kind='hist', bins=(nx, ny))
        g.ax_marg_x.cla()
        g.ax_marg_y.cla()

        # interior grid heatmap on top of skeleton
        heat = sns.heatmap(data=stack[weight].to_numpy().reshape(ny, nx), ax=g.ax_joint, cbar=False, cmap='Blues', square=True)

        # set values for totals bars
        g.ax_marg_x.bar(np.arange(0.5, nx), stack.groupby([x])[weight].sum().to_numpy(), color='#2697de')
        g.ax_marg_y.barh(np.arange(0.5, ny), stack.groupby([y])[weight].sum().to_numpy(), color='#2697de')

        # # remove ticks between heatmao and histograms
        g.ax_marg_x.tick_params(axis='x', bottom=False, labelbottom=False)
        g.ax_marg_y.tick_params(axis='y', left=False, labelleft=False)
        # remove ticks showing the heights of the histograms
        g.ax_marg_x.tick_params(axis='y', left=False, labelleft=False)
        g.ax_marg_y.tick_params(axis='x', bottom=False, labelbottom=False)
        # less space needed when tick labels are removed
        g.fig.subplots_adjust(hspace=0.05, wspace=0.02)

        # tick parameters
        heat.set_xticklabels(xtick_labels, rotation=xtick_rotation)
        heat.set_yticklabels(ytick_labels, rotation=ytick_rotation)
        heat.set_xlabel('(Local Time)', size=8)

        # set final size as a ratio of nx and ny
        g.fig.set_size_inches(nx/3.5, ny/3.5)

        if return_bytes:
            buffer = BytesIO()
            plt.savefig(buffer, dpi=300, bbox_inches='tight', pad_inches=0.1)
            plt.close()
            buffer.seek(0)
            return buffer
        if export_path != None:
            plt.savefig(fname=export_path, dpi=300, bbox_inches='tight', pad_inches=0.1)
            plt.close()
        else:
            plt.show()
    
    def bullet_current_performance(self):
        """
        Returns a bullet point describing the traffic of the most recent week relative to the average.
        """
        latest_week = self.obs.iloc[-1]
        best_week = self.obs.iloc[self.obs['traffic_pct'].idxmax()]
        worst_week = self.obs.iloc[self.obs['traffic_pct'].idxmin()]

        # format & round current week performance percentage
        latest_performance = latest_week['traffic_pct']
        
        if abs(latest_performance) > 1:
            latest_performance = round(latest_performance)
        else:
            latest_performance = round(latest_performance,1)
        
        # current week is new best
        if latest_week['Week'] == best_week['Week']:
            bullet = f"Week {latest_week['Week']} has set a new high in market traffic within the last 4 months, with {latest_performance}% above the average."
        # current week is new worst
        elif latest_week['Week'] == worst_week['Week']:
            bullet = f"Week {latest_week['Week']} has set a new low in market traffic within the last 4 months, with {latest_performance}% below the average."
        # current week is above average
        elif latest_performance >= 0:
            bullet = f"This market is currently {latest_performance}% above the average level of traffic observed over the past 4 months."
        # current week is below average
        elif latest_performance < 0:
            bullet = f"This market is currently {abs(latest_performance)}% below the average level of traffic observed over the past 4 months."
                
        return bullet

    def bullet_continuous_growth(self):
        """
        Returns a bullet point describing the longest period of consecutive growth.
        """
        latest_week = self.obs.iloc[-1]
        max_weeks_of_growth = self.obs['Weeks of Growth'].max()

        # get most recent max
        rev = self.obs.sort_index(ascending=False)
        recent_max = self.obs.iloc[rev['Weeks of Growth'].idxmax()]
        
        # if latest growth period ends in current week
        if (recent_max['Week'] == latest_week['Week']):
            bullet = f"As of Week {recent_max['Week']} {dates_to_range(recent_max['EarliestDate'], recent_max['LatestDate'])} there have been {max_weeks_of_growth} consecutive weeks of growth."
        # if latest growth period is no longer active
        else:
            bullet = f"The longest period of sustained growth ended in Week {recent_max['Week']} after {max_weeks_of_growth} consecutive weeks of growth."
                
        return bullet

    def bullet_six_weeks(self):
        """
        Returns a bullet point describing the performance of the last 6 weeks compared to the 4 month average.
        """
        bullet = ''
        # if sustained growth was met, calculate recent six-weeks overall growth
        six_weeks = self.obs.iloc[-6:].copy()

        # recent weeks overperforming the market average
        over = six_weeks[six_weeks['traffic_pct'] > 0]

        bullet = f"This market has outperformed its average traffic level in {str(len(over))} of the last 6 weeks."
        if len(over) == 1:
            bullet = bullet.replace('times','time')

        return bullet

       
    def bullet_budget(self):
        """
        Returns a bullet detailing the budget recommendation based on the past N weeks performance.
        """
        N = 6
        very_threshold = 30
        recent_performance = self.obs.iloc[-N:]['traffic_pct'].mean()

        # very positive
        if recent_performance > very_threshold:
            bullet = 'We recommend raising advertising budget to take advantage of an increasing market.'
        # moderately positive
        elif recent_performance > 0:
            bullet = 'We recommend strategic increases in advertising budget while the market continues to grow.'
        # moderately negative
        elif recent_performance > -very_threshold:
            bullet = 'We recommend a moderate level of advertising budget with an option for increase as the market continues to stabilize.'
        # very negative
        else:
            bullet = 'We recommend maintaining a conservative budget until the market stabilizes fully.'
            
        return bullet

    
    def stability_score(self):
        """
        Returns a score ranking the week-to-week stability of the data, on a scale from 0 to 1.
        """
        std_dev = self.obs['traffic_pct'].std()
        stability = 100 - (10 * (std_dev**0.4)) # decaying power equation
        
        # round and set floor/ceiling
        stabscore = round(stability/100, 2)
        stabscore = min(stabscore, 1)
        stabscore = max(0, stabscore)

        return stabscore

    def trend_score(self):
        """
        Returns a score ranking the overall trend of the data, on a scale from 0 to 1.
        """
        trendscore = 50 + trendline(self.obs['traffic_pct'])*6

        # round and set floor/ceiling
        trendscore = round(trendscore/100,2)
        trendscore = min(trendscore, 1)
        trendscore = max(0, trendscore)
        
        return trendscore

    def recent_score(self):
        """
        Returns a score ranking the performance of the most recent 6 weeks, on a scale from 0 to 1.
        """
        recentscore = 50 + self.obs[-6:]['traffic_pct'].mean()

        # round and set floor/ceiling
        recentscore = round(recentscore/100,2)
        recentscore = min(recentscore, 1)
        recentscore = max(0, recentscore)

        return recentscore


    def condition_week_over_week(self):
        """
        Returns the check, week, and range for the week over week condition.
        """
        # week over week condition
        week_over_week_threshold = 0.095
        for i, row in self.obs.iterrows():
            if int(i) > 0:
                self.obs.loc[int(i), 'week over week'] = (row['deviceid'] / self.obs.loc[int(i)-1, 'deviceid']) - 1

        ten_pct_increases = self.obs[self.obs['week over week']>week_over_week_threshold]

        if len(ten_pct_increases) > 0:
            recent_increase = ten_pct_increases.iloc[-1]
            check = u'\u2713'
            caption = 'Week ' + str(recent_increase['Week']) + ' ' + dates_to_range(recent_increase['EarliestDate'], recent_increase['LatestDate'])
        else:
            check = 'X'
            caption = 'Condition Not Satisfied'
            
        return check, caption

    
    def condition_positive_trend(self):
        """
        Returns the check, and caption for the positive trend condition.
        """
        # get min max condition and earliest week satisified
        if trendline(self.obs['deviceid']) > 0:
            check = u'\u2713'
            caption = 'Positive 24-Week Trend'
        else:
            check = 'X'
            caption = 'Condition Not Satisfied'

        return check, caption

   
    def condition_sustained_growth(self):
        """
        Returns the check, week, and range for the sustained growth condition.
        """
        six_weeks = self.obs.iloc[-6:]
        
        # if there has been 2-week sustained growth within the last 6 weeks
        if six_weeks['Weeks of Growth'].max() >= 2:
            # return the latest such instnace
            latest_growth = six_weeks[six_weeks['Weeks of Growth']>=2].iloc[-1]
            check = u'\u2713'
            caption = 'Week ' + str(latest_growth['Week']) + ' ' + dates_to_range(latest_growth['EarliestDate'], latest_growth['LatestDate'])
        
        # if no such sustained growth
        else:
            check = 'X'
            caption = 'Condition Not Satisfied'

        return check, caption


### MISCELLANEOUS ULTILTY FUNCTIONS ###

def ytick_formatter(x, pos):
    """
    Plus/Minus percentage formatter for the yaxis of the foot traffic graph.
    """
    s = str(round(x))
    label = '+'+s+'%' if x > 0 else '-'+s+'%' if x < 0 else s+'%'
    return label

def get_week(date):
    """
    Returns the Mon-Sun week number.
    """
    week = dt.strftime(date, '%W')
    if week == '00':
        week = '53'
    return week

def get_date_by_week_offset(date, offset):
    """
    Returns the date at position N of the given Mon-Sun week.

    Ex. n=1 would return the Tuesday of that week, n=6 would return the Sunday.
    """
    ref_date = date - timedelta(days=date.weekday()) + timedelta(days=offset)

    return ref_date

def dates_to_range(start, end):
    """
    Converts a start and end date to a formatted date range string.
    """
    start_str = dt.strftime(start, '%#m/%#d')
    end_str = dt.strftime(end, '%#m/%#d')

    return f"({start_str}-{end_str})"

def plus_padding(x):  
    """
    Adds a + sign to the front of positive numbers.
    """
    # add plus padding
    if x >= 0:
        x = '+' + str(x)  
    else:
        x = str(x)
    # add space padding if single digit
    if len(x) < 3:
        x = ' ' + x
    return x

def trendline(series, order=1):
    """
    Returns slope of a numpy series.
    """
    coeffs = np.polyfit(series.index.values, list(series), order)
    slope = coeffs[-2]
    return float(slope)

def sort_by_list(column, sort_list):
    """
    Utility function for sorting rows in a pandas dataframe by a list of values.
    """
    correspondence = {item: idx for idx, item in enumerate(sort_list)}
    return column.map(correspondence)

def _parse_iso_datetime_utc(datetime_series: pd.Series) -> pd.Series:
    from dateutil import parser
    return datetime_series.apply(lambda x: parser.isoparse(x).astimezone(tz=pd.Timestamp.utcnow().tz))