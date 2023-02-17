import logging
import os

import nltk
from serpapi import GoogleSearch
from spellchecker import SpellChecker
from google import GoogleSearch
from youtube import YoutubeSearch

logger = logging.getLogger(__name__)


class WebScraper:
    def __init__(self, language: str = "es") -> None:
        nltk.download(info_or_id="stopwords", download_dir="/tmp", raise_on_error=True)

        try:
            self.spell = SpellChecker(language=language)

            self.nlp_word_embedding = []
        except Exception as e:
            logger.exception(e)

    def running_word_embeddings_with_dataframe_columns(
        self, dataframe
    ):
        """
        Busca contenido con referencia a
        keyword, competence_id, interest_area_id, thematic
        """
        try:
            
            self.training_need_id = dataframe["id"].values[0]
            self.interest_area_id = dataframe["interest_area_id"].values[0]
            self.competence_id = dataframe["competence_id"].values[0]
            
            dataframe = dataframe[
                [
                    "thematic",
                    "keyword",
                    "description",
                    "description_competence",
                    "description_interest_area",
                ]
            ]

            keywords = list(dataframe.columns)

            for keyword in keywords:
                keywords = dataframe[keyword].values[0]
                self.word_embeddings(keywords)
        except Exception as e:
            logger.exception(e)

    def word_embeddings(self, keywords: str, language: str = "spanish"):
        from nltk.corpus import stopwords
        nltk.download('punkt')
        
        from nltk.tokenize import word_tokenize

        try:
            stop_words = set(stopwords.words(language)) 
            word_tokens = word_tokenize(keywords)
            filtered_sentence = [w for w in word_tokens if not w in stop_words]
            
            filtered_sentence = [] 
            
            import numpy as np 
            
            for w in word_tokens: 
                if w not in stop_words and w!= keywords and len(w) > 3:
                    filtered_sentence.append(w)
            
            
            if len(filtered_sentence) > 0:
                filtered_sentence = np.array(filtered_sentence)
                filtered_sentence = filtered_sentence[str(filtered_sentence)!=","]
                filtered_sentence = filtered_sentence.tolist()
                
                # Para generar n-grams, en particular 3-grams
                
                nlp_word_embedding = list(set(filtered_sentence[0]))
                nlp_word_embedding = nltk.word_tokenize(" ".join(nlp_word_embedding)) 
                nlp_word_embedding = list(nltk.ngrams(nlp_word_embedding,3))
                
                for word in nlp_word_embedding:
                    nlp_word = " ".join(word)
                    self.nlp_word_embedding.append(nlp_word)

                self.nlp_word_embedding = list(set(self.nlp_word_embedding))
    
        except Exception as e:
            logger.exception(e)

    def start_scrappy_requests(self, keywords: list, limit: int = 100):
        try:
            
            #  Google
            google = GoogleSearch()
            google_results = google.search_google(keywords, limit)
            
            # Youtube
            youtube = YoutubeSearch()
            youtube_results = youtube.search_youtube(keywords, limit)
    
            return google_results, youtube_results

        except Exception as e:
            logger.exception(e)

    def normalize(self, string: str):
        try:
            replacements = (
                ("á", "a"),
                ("é", "e"),
                ("í", "i"),
                ("ó", "o"),
                ("ú", "u"),
            )
            for a, b in replacements:
                string = string.replace(a, b).replace(a.upper(), b.upper())
            return string
        except Exception as e:
            logger.exception(e)
