import scispacy
import spacy
import numpy as np
from loguru import logger


def mark_as_complete(name):
    f = open(name, "w")
    f.write("Done")
    f.close()
