import pandas as pd
from nexus.corr_analysis.llm.assistant import CorrelationAssistant
from nexus.corr_analysis.llm.chatbot import ChatBot
from nexus.data_search.search_corr import Correlation
from nexus.utils.io_utils import dump_json, load_json
import time

def test_generating_context_around_a_correlation():
    correlations = pd.read_csv('demo/00021_Social_Chicago_correlations_sampled_20.csv')
    # iterate over the rows of the dataframe
    for index, row in correlations.iterrows():
        profile = load_json('tmp/llm_assistant/correlation_{}.json'.format(index))
        # instantiate the assistant
        assistant = CorrelationAssistant(correlation=Correlation.from_csv(row), client=ChatBot(model='mistral'))
        # summarize the correlation
        # profile['summary'] = assistant.summarize()
        # # explain the correlation
        # profile['explanation'] = assistant.explain()
        # # interesting classification of the correlation
        profile['interestingness_tag_2'] = assistant.interesting_classification(mode='generate')
        # interesting classification of the correlation with explanation
        # profile['interestingness_reason'] = assistant.interesting_classification_with_explanation()
        # causal classification of the correlation
        profile['causal_tag_2'] = assistant.causal_classification(mode='generate')
        # causal classification of the correlation with explanation
        # profile['causal_tag_reason'] = assistant.causal_classification_with_explanation()
        # confounder of the correlation
        # profile['confounder'] = assistant.confounder()
        dump_json(f'tmp/llm_assistant/correlation_{index}.json', profile)

if __name__ == "__main__":
    start = time.time()
    test_generating_context_around_a_correlation()
    print("Time taken: ", time.time() - start, " seconds.")
    # for i in range(20):
    #     data = load_json('tmp/llm_assistant/correlation_{}.json'.format(i))
    #     print(data["causal_tag"])