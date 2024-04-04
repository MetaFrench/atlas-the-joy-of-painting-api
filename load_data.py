import pandas as pd
from sqlalchemy import create_engine
import re

engine = create_engine("mysql+pymysql://root:1234@localhost/joy_of_painting")


colorData = pd.read_csv(
    './The Joy Of Painiting - Colors Used', on_bad_lines='skip')
episode_elements = pd.read_csv(
    './The Joy Of Painiting - Subject Matter', on_bad_lines='skip')
episode_date = pd.read_csv('./The Joy Of Painting - Episode Dates', on_bad_lines='skip',
                           delimiter='[()]', names=['episode', 'date', 'desc'], engine='python')

for el in episode_date.iterrows():
    el[1].date = el[1].date.split()[0]

for n in colorData.colors:
    n = re.findall("'.*'", n)

colorData.to_sql('color_elements', con=engine, if_exists='replace')
episode_elements.to_sql('episode_elements', con=engine, if_exists='replace')
episode_date.to_sql('episode_date', con=engine, if_exists='replace')

nix_list_colorData = ['index', 'painting_index', 'img_src', 'painting_title', 'season', 'episode', 'num_colors', 'youtube_src',
                      'color_hex', 'Black_Gesso', 'Bright_Red', 'Burnt_Umber', 'Cadmium_Yellow', 'Dark_Sienna', 'Indian_Red',
                      'Indian_Yellow', 'Liquid_Black', 'Liquid_Clear', 'Midnight_Black', 'Phthalo_Blue', 'Phthalo_Green',
                      'Prussian_Blue', 'Sap_Green', 'Titanium_White', 'Van_Dyke_Brown', 'Yellow_Ochre', 'Alizarin_Crimson']

nix_list_epi_date = ['episode']

combined_data = pd.concat([colorData.drop(nix_list_colorData, axis=1), episode_date.drop(
    nix_list_epi_date, axis=1), episode_elements], axis=1)

combined_data = combined_data[['EPISODE', 'TITLE', 'date', 'colors', 'AURORA_BOREALIS',
                               'BARN', 'BEACH', 'BOAT', 'BRIDGE', 'BUILDING', 'BUSHES', 'CABIN', 'CACTUS',
                               'CIRCLE_FRAME', 'CIRRUS', 'CLIFF', 'CLOUDS', 'CONIFER', 'CUMULUS',
                               'DECIDUOUS', 'DIANE_ANDRE', 'DOCK', 'DOUBLE_OVAL_FRAME', 'FARM',
                               'FENCE', 'FIRE', 'FLORIDA_FRAME', 'FLOWERS', 'FOG', 'FRAMED', 'GRASS',
                               'GUEST', 'HALF_CIRCLE_FRAME', 'HALF_OVAL_FRAME', 'HILLS', 'LAKE', 'LAKES',
                               'LIGHTHOUSE', 'MILL', 'MOON', 'MOUNTAIN', 'MOUNTAINS', 'NIGHT', 'OCEAN',
                               'OVAL_FRAME', 'PALM_TREES', 'PATH', 'PERSON', 'PORTRAIT', 'RECTANGLE_3D_FRAME',
                               'RECTANGULAR_FRAME', 'RIVER', 'ROCKS', 'SEASHELL_FRAME', 'SNOW', 'SNOWY_MOUNTAIN',
                               'SPLIT_FRAME', 'STEVE_ROSS', 'STRUCTURE', 'SUN', 'TOMB_FRAME', 'TREE', 'TREES',
                               'TRIPLE_FRAME', 'WATERFALL', 'WAVES', 'WINDMILL', 'WINDOW_FRAME', 'WINTER', 'WOOD_FRAMED']]

combined_data.to_sql('main_data', con=engine, if_exists='replace')
