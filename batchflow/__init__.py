from loguru import logger

# add new level TIME
# this level is used to log time
time_level = logger.level("TIME", no=39, color="<green>")
