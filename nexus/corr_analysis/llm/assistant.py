from nexus.data_search.search_corr import Correlation
from nexus.corr_analysis.llm.chatbot import ChatBot
from typing import List
import pandas as pd

class CorrelationAssistant:
    def __init__(self, correlation: Correlation, client: ChatBot) -> None:
        self.client = client
        self.correlation = correlation
        self.attr1 = correlation.agg_col1.agg_attr
        self.table1 = correlation.agg_col1.tbl_name
        self.desc1 = correlation.agg_col1.desc if correlation.agg_col1.desc != "" else "not provided"
        self.attr2 = correlation.agg_col2.agg_attr
        self.table2 = correlation.agg_col2.tbl_name
        self.desc2 = correlation.agg_col2.desc if correlation.agg_col2.desc != "" else "not provided"
        self.r_val = self.correlation.r_val

    def summarize(self):
        question = f"attribute {self.attr1} from table {self.table1} is correlated with \
            attribute {self.attr2} from {self.table2}. \
            The definition of {self.attr1} is {self.desc1} and the definition of {self.attr2} is {self.desc2}. \
            The correlation coefficient is {self.r_val}. \
            Can you summarize this correlation in a single sentence? Your goal is to help a person grasp it quickly."
        return self.client.chat(msg=question, options={"seed": 123, "temperature": 0})

    def explain(self):
        question = "Provide a brief narrative or explanation of why this correlation exists."
        return self.client.chat(msg=question, options={"seed": 123, "temperature": 0})

    def reference(self):
        question = """
            Provide me with literature that investigate this correlation? \
            Please use information from attributable sources."
        """
        return self.client.chat(msg=question, options={"seed": 123, "temperature": 0})

    def confounder(self):
        question = "What are the potential confounders that could explain this correlation?"
        return self.client.chat(msg=question, options={"seed": 123, "temperature": 0})

    def interesting_classification(self, mode: str = "generate"):
        if mode == "chat":
            question = "Imagine you are a social scientist looking for a research topic, \
                        do you find this correlation interesting and worth furthur investigation? \
                        even if you are uncertain, you must pick an answer from yes or no without using another words."
            return self.client.chat(msg=question, options={"seed": 123, "temperature": 0})
        elif mode == "generate":
            system_msg = "Imagine you are a social scientist looking for an interesting research topic."
            correlation_desc = f"attribute {self.attr1} from table {self.table1} is correlated with \
                                attribute {self.attr2} from {self.table2}. \
                                The definition of {self.attr1} is {self.desc1} and the definition of {self.attr2} is {self.desc2}. \
                                The correlation coefficient is {self.r_val}."
            question = f"""Which option best describes this correlation? 
                
                    A. Can lead to a research topic.
                    B. Not surprising.
                
                Let's work this out in a step by step way to be sure that we have the right answer. Then provide your final answer within tags <Answer>A/B</Answer>.
                """
            return self.client.generate(msg=correlation_desc + question, system_msg=system_msg,
                                        options={"seed": 123, "temperature": 0})

    def interesting_classification_with_explanation(self, mode: str = "generate"):
        if mode == "chat":
            question = "Do you find this correlation interesting or not? Even if you are uncertain, \
                        you must pick an answer from yes or no. Explain your answer."
            return self.client.chat(msg=question, options={"seed": 123, "temperature": 0})
        elif mode == "generate":
            system_msg = "You are a helpful assistant for causal reasoning."
            correlation_desc = f"attribute {self.attr1} from table {self.table1} is correlated with \
                                attribute {self.attr2} from {self.table2}. \
                                The definition of {self.attr1} is {self.desc1} and the definition of {self.attr2} is {self.desc2}. \
                                The correlation coefficient is {self.r_val}.\n"
            question = f"""Which cause-and-effect relationship is more likely? Consider only direct causal mechenism and ignore any effect\
                due to common causes.\
                
                    A. {self.attr1} causes {self.attr2}.
                    B. {self.attr2} causes {self.attr1}.
                    C. No causal relationship exists.\
                
                Let's work this out in a step by step way to be sure that we have the right answer. Then provide your final answer within tags <Answer>A/B/C</Answer>.
                """

            return self.client.generate(msg=correlation_desc + question, system_msg=system_msg,
                                        options={"seed": 123, "temperature": 0})

    def causal_classification(self, mode: str = 'generate'):
        if mode == "chat":
            question = "Is there a causal link between the two variables? even if you are uncertain, \
                        you must pick an answer from yes or no without using another words."
            return self.client.chat(msg=question, options={"seed": 123, "temperature": 0})
        elif mode == "generate":
            system_msg = "You are a helpful assistant for causal reasoning."
            correlation_desc = f"attribute {self.attr1} from table {self.table1} is correlated with \
                                attribute {self.attr2} from {self.table2}. \
                                The definition of {self.attr1} is {self.desc1} and the definition of {self.attr2} is {self.desc2}. \
                                The correlation coefficient is {self.r_val}.\n"
            question = f"""Which cause-and-effect relationship is more likely? Consider only direct causal mechenism and ignore any effect\
                due to common causes.\
                
                    A. {self.attr1} causes {self.attr2}.
                    B. {self.attr2} causes {self.attr1}.
                    C. No causal relationship exists.\
                
                Let's work this out in a step by step way to be sure that we have the right answer. Then provide your final answer within tags <Answer>A/B/C</Answer>."""

            return self.client.generate(msg=correlation_desc + question, system_msg=system_msg,
                                        options={"seed": 123, "temperature": 0})

    def causal_classification_with_explanation(self):
        question = "Is there a causal link between the two variables? Even if you are uncertain, \
                    you must pick an answer from yes or no. Explain your answer."
        return self.client.chat(msg=question, options={"seed": 123, "temperature": 0})

    @staticmethod
    def suggest_variables_to_control_for(client, correlations_df: pd.DataFrame):
        correlations = []
        for i in range(len(correlations_df)):
            correlations.append(Correlation.from_csv(correlations_df.iloc[i]))
        variable_dict = {}
        for correlation in correlations:
            attr1 = correlation.agg_col1.agg_attr
            table1 = correlation.agg_col1.tbl_name
            desc1 = correlation.agg_col1.desc if correlation.agg_col1.desc != "" else "not provided"
            attr2 = correlation.agg_col2.agg_attr
            table2 = correlation.agg_col2.tbl_name
            desc2 = correlation.agg_col2.desc if correlation.agg_col2.desc != "" else "not provided"
            variable_dict[(attr1, table1)] = desc1
            variable_dict[(attr2, table2)] = desc2
        
        variable_explanations = "Here is the explanation of the variables:\n" + \
                                "\n".join([f"The description of {attr} from {table} is {desc}."\
                                          for (attr, table), desc in variable_dict.items()])
        question = "Given the following correlations, which variables would you suggest controlling for to reduce the number of correlations?\n"
        correlation_desc = "\n".join([f"{correlation.agg_col1.agg_attr} from {correlation.agg_col1.tbl_name} is correlated with \
                                        {correlation.agg_col2.agg_attr} from {correlation.agg_col2.tbl_name}. \
                                       The correlation coefficient is {correlation.r_val}." for correlation in correlations])
        system_msg = "You are a helpful assistant for causal reasoning."
        return client.generate(msg=variable_explanations + question + correlation_desc, system_msg=system_msg,
                                        options={"seed": 123, "temperature": 0})
