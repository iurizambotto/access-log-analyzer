import os
from pyspark.sql import SparkSession

from dateparser import parse
import calendar

from argparse import ArgumentParser


def get_args():
    parser = ArgumentParser()
    parser.add_argument(
        "--input_path", type=str, default="/opt/spark-data/data/access_log.txt"
    )
    parser.add_argument(
        "--output_path", type=str, default="/opt/spark-data/output.txt"
    )
    return parser.parse_args()


def _try_cast(obj, type):
    try:
        return isinstance(type(obj), type)
    except:
        return False


def add_to_output_file(file_path, question, answer):
    output_path_dir = "/".join(file_path.split("/")[:-1])
    if not os.path.exists(output_path_dir):
        os.makedirs(output_path_dir)

    with open(file_path, "a") as file:
        file.write(f"{question}\nR: {answer}\n\n")


def get_answer_from_question_1(rdd, output_file_path):
    '''
    This function gets the answer for question 1: return the 10 most accessed IPs
    args:
        rdd: spark RDD
        output_file_path: str
    return:
        question_1: list
    
    '''
    question = "Questão 1) Identifique as 10 maiores origens de acesso (Client IP) por quantidade de acessos."
    question_1 = (
        rdd.map(
            lambda line: (
                line["ip_address"],
                1,
            )
        )
        .reduceByKey(lambda k, v: k + v)
        .map(lambda item: tuple(list(item)[::-1]))
        .sortByKey(False)
        .map(lambda item: item[1])
        .take(10)
    )
    print(
        "\nQUESTION RESPONSE",
        question,
        "\nR:",
        f"Os principais IPs de origem são: {question_1}",
    )
    add_to_output_file(
        output_file_path,
        question,
        f"Os principais IPs de origem são: {question_1}",
    )
    return question_1


def get_answer_from_question_2(rdd, output_file_path):
    ## Questão 2) Liste os 6 endpoints mais acessados, desconsiderando aqueles que representam arquivos
    question = "Questão 2) Liste os 6 endpoints mais acessados, desconsiderando aqueles que representam arquivos"
    question_2 = (
        rdd.filter(lambda item: "." not in item.get("path"))
        .map(
            lambda line: (
                line["path"],
                1,
            )
        )
        .reduceByKey(lambda k, v: k + v)
        .map(lambda item: tuple(list(item)[::-1]))
        .sortByKey(False)
        .map(lambda item: item[1])
        .take(6)
    )
    print(
        "\nQUESTION RESPONSE",
        question,
        "\nR:",
        f"Os principais endpoints são: {question_2}",
    )
    add_to_output_file(
        output_file_path,
        question,
        f"Os principais endpoints são: {question_2}",
    )
    return question_2


def get_answer_from_question_3(rdd, output_file_path):
    ## Questão 3) Qual a quantidade de Client IPs distintos
    question = "Questão 3) Qual a quantidade de Client IPs distintos"
    question_3 = rdd.map(lambda line: line["ip_address"]).distinct().count()
    print(
        "\nQUESTION RESPONSE",
        question,
        "\nR:",
        f"{question_3} Client IPs distintos",
    )
    add_to_output_file(
        output_file_path, question, f"{question_3} Client IPs distintos"
    )
    return question_3


def get_answer_from_question_4(rdd, output_file_path):
    ## Questão 4) Quantos dias de dados estão representados no arquivo?
    question = (
        "Questão 4) Quantos dias de dados estão representados no arquivo?"
    )
    question_4 = (
        rdd.map(lambda line: line["datetime"].split(":")[0]).distinct().count()
    )
    print(
        "\nQUESTION RESPONSE",
        question,
        "\nR:",
        f"{question_4} dias",
    )
    add_to_output_file(output_file_path, question, f"{question_4} dias")
    return question_4


def get_answer_from_question_5_mean(question_5_rdd, output_file_path):
    ### O volume médio de dados retornado
    question = "Questão 5d) O volume médio de dados retornado"
    question_5_rdd_mean = question_5_rdd.mean()
    print(
        "\nQUESTION RESPONSE",
        question,
        "\nR:",
        f"{question_5_rdd_mean} Bytes ou {round(question_5_rdd_mean / 1024 / 1024, 2)} Megabytes",
    )
    add_to_output_file(
        output_file_path,
        question,
        f"{question_5_rdd_mean} Bytes ou {round(question_5_rdd_mean / 1024 / 1024, 2)} Megabytes",
    )
    return question_5_rdd_mean


def get_answer_from_question_5_min(question_5_rdd, output_file_path):
    ### O menor volume de dados em uma única resposta.
    question = "Questão 5c) O menor volume de dados em uma única resposta."
    question_5_rdd_min = question_5_rdd.min()
    print(
        "\nQUESTION RESPONSE",
        question,
        "\nR:",
        f"{question_5_rdd_min} Bytes ou {round(question_5_rdd_min / 1024 / 1024, 2)} Megabytes",
    )
    add_to_output_file(
        output_file_path,
        question,
        f"{question_5_rdd_min} Bytes ou {round(question_5_rdd_min / 1024 / 1024, 2)} Megabytes",
    )
    return question_5_rdd_min


def get_answer_from_question_5_max(question_5_rdd, output_file_path):
    ### O maior volume de dados em uma única resposta.
    question = "Questão 5b) O maior volume de dados em uma única resposta."
    question_5_rdd_max = question_5_rdd.max()
    print(
        "\nQUESTION RESPONSE",
        question,
        "\nR:",
        f"{question_5_rdd_max} Bytes ou {round(question_5_rdd_max / 1024 / 1024, 2)} Megabytes",
    )
    add_to_output_file(
        output_file_path,
        question,
        f"{question_5_rdd_max} Bytes ou {round(question_5_rdd_max / 1024 / 1024, 2)} Megabytes",
    )
    return question_5_rdd_max


def get_answer_from_question_5_sum(question_5_rdd, output_file_path):
    ### O volume total de dados retornado.
    question = "Questão 5a) O volume total de dados retornado."
    question_5_rdd_sum = question_5_rdd.sum()
    print(
        "\nQUESTION RESPONSE",
        "a. O volume total de dados retornado.",
        "\nR:",
        f"{question_5_rdd_sum} Bytes ou {round(question_5_rdd_sum / 1024 / 1024, 2)} Megabytes ou {round(question_5_rdd_sum / 1024 / 1024 / 1024, 2)} Gigabytes ou {round(question_5_rdd_sum / 1024 / 1024 / 1024 / 1024, 2)} Terabytes",
    )
    add_to_output_file(
        output_file_path,
        question,
        f"{question_5_rdd_sum} Bytes ou {round(question_5_rdd_sum / 1024 / 1024, 2)} Megabytes ou {round(question_5_rdd_sum / 1024 / 1024 / 1024, 2)} Gigabytes ou {round(question_5_rdd_sum / 1024 / 1024 / 1024 / 1024, 2)} Terabytes",
    )
    return question_5_rdd_sum


def get_answer_from_question_5(rdd, output_file_path):
    ## Questão 5) Com base no tamanho (em bytes) do conteúdo das respostas, faça a seguinte análise
    question = "Questão 5) Com base no tamanho (em bytes) do conteúdo das respostas, faça a seguinte análise"
    print(
        "\nQUESTION RESPONSE",
        question,
    )
    add_to_output_file(output_file_path, question, "Seguem as respostas abaixo:")
    question_5_rdd = (
        rdd.filter(
            lambda item: (item.get("response_code", "").startswith("2"))
        )
        .filter(lambda item: _try_cast(item.get("response_size", ""), int))
        .map(lambda item: int(item.get("response_size")))
    )
    question_5_rdd_sum = get_answer_from_question_5_sum(
        question_5_rdd, output_file_path
    )
    question_5_rdd_max = get_answer_from_question_5_max(
        question_5_rdd, output_file_path
    )
    question_5_rdd_min = get_answer_from_question_5_min(
        question_5_rdd, output_file_path
    )
    question_5_rdd_mean = get_answer_from_question_5_mean(
        question_5_rdd, output_file_path
    )
    return (
        question_5_rdd_sum,
        question_5_rdd_max,
        question_5_rdd_min,
        question_5_rdd_mean,
    )


def get_answer_from_question_6(rdd, output_file_path):
    ## Questão 6) Qual o dia da semana com o maior número de erros do tipo "HTTP Client Error"?
    # Ref: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status#client_error_responses
    question = "Questão 6) Qual o dia da semana com o maior número de erros do tipo 'HTTP Client Error'?"
    question_6 = (
        rdd.filter(lambda item: item.get("response_code", "").startswith("4"))
        .map(
            lambda line: (
                calendar.day_name[
                    parse(
                        line["datetime"].split(":")[0].replace("[", "")
                    ).weekday()
                ],
                1,
            )
        )
        .reduceByKey(lambda k, v: k + v)
        .map(lambda item: tuple(list(item)[::-1]))
        .sortByKey(False)
        .map(lambda item: item[1])
        .take(1)[0]
    )
    print(
        "\nQUESTION RESPONSE",
        "\n",
        question,
        "\nR:",
        f"{question_6} é o dia da semana com o maior número de erros do tipo 'HTTP Client Error'",
    )
    add_to_output_file(
        output_file_path,
        question,
        f"{question_6} é o dia da semana com o maior número de erros do tipo 'HTTP Client Error'",
    )
    return question_6


if __name__ == "__main__":
    args = get_args()
    path = args.input_path
    output_file_path = args.output_path

    spark = SparkSession.builder.appName("accessLogAnalysis").getOrCreate()
    sc = spark.sparkContext

    columns = [
        "ip_address",
        "1",
        "2",
        "datetime",
        "timezone",
        "method",
        "path",
        "type",
        "response_code",
        "response_size",
    ]

    rdd = sc.textFile(path).map(lambda line: dict(zip(columns, line.split())))

    # get the answers
    question_1 = get_answer_from_question_1(
        rdd=rdd, output_file_path=output_file_path
    )
    question_2 = get_answer_from_question_2(
        rdd=rdd, output_file_path=output_file_path
    )
    question_3 = get_answer_from_question_3(
        rdd=rdd, output_file_path=output_file_path
    )
    question_4 = get_answer_from_question_4(
        rdd=rdd, output_file_path=output_file_path
    )
    question_5 = get_answer_from_question_5(
        rdd=rdd, output_file_path=output_file_path
    )
    question_6 = get_answer_from_question_6(
        rdd=rdd, output_file_path=output_file_path
    )
