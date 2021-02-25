from faker import Faker
import random
from datetime import datetime
from pathlib import Path
import csv
from typing import List
import boto3

"""Call ID AND Arrive time"""


def prepared(fake):
    """Prepared some fields for database"""
    gender = random.choice(["Male", "Female", "Non-binary"])
    insurance_types = random.choice(["Bronze", "Silver", "Gold", "No"])
    types = random.choice(["spes", "usual ", "ambul"])
    description = random.choice(
        [("cough", "fever"), "fever", "cough", "blood pressure", "heart attack", "hypertensive crisis", "fire",
         "ACCIDENT", "hypotonic crises", "abdominal pain", "dyspnoea", "Gasping for breath",
         "A complication of diabetes", "Blood loss", "childbirth", "Thermal burns", "attempted suicide",
         "schizo", "shoe", "bipolar disorder"])
    name = [("Авремен В.В.", 1), ("Валк Е.У.", 2), ("Валук Х.У.", 3), ("Гейра К.У.", 4),
            ("Жизна В.А.", 5), ("Куплинов А.А.", 6), ("Майстрова Д.М.", 7), ("Мыхина А.А.", 8),
            ("Немечко Ж.У.", 9), ("Павелк А.Ф", 10), ("Прокопенко Ю.И.", 11), ("Юлинка О.В.", 12),
            ("Бузенков", 13), ("Дуркина В.Ф.", 14), ("Куриан А.В.", 15), ("Мазепа А.В.", 16),
            ("Палкова В.Ы.", 17), ("Пирухин В.Э.", 18), ("Шах Ф.В.", 19), ("Бирзок В.Я.", 20),
            ("Буликов В.У.", 21), ("Жак М.Я.", 22), ("Жировкин У.Х.", 23), ("Замов В.Ц.", 25),
            ("Набиулина А.Я.", 26), ("Первая А.А", 27), ("Перенков В.У.", 28), ("Раков В.Ц.", 29),
            ("Райнер В.У.", 30), ("Бурлин А.А.", 31), ("Бурят А.В.", 32), ("Жилко А.Ц.", 33),
            ("Мирюх В.У.", 34), ("Мурлин В.Е.", 35), ("Пальник В.Ф.", 36), ("Травмоман П.У.", 37)]
    paramedic = random.choice(name[:12])
    paramedic2 = random.choice(name[:12])
    nurse = random.choice(name[12:20])
    driver = random.choice(name[20:31])
    doctor = random.choice(name[31:38])
    if gender == "Male":
        customer = {
            "first_name": fake.first_name_male(),
            "last_name": fake.last_name_male(),
        }
    elif gender == "Female":
        customer = {
            "first_name": fake.first_name_female(),
            "last_name": fake.last_name_female(),
        }
    else:
        customer = {
            "first_name": fake.first_name_nonbinary(),
            "last_name": fake.last_name_nonbinary(),
        }
    birthday = fake.date_between_dates(date_start=datetime(1971, 1, 1),
                                       date_end=datetime(2020, 1, 1)).strftime('%Y-%m-%d')
    arrive_time = fake.date_time_between_dates(datetime_start=datetime(2021, 1, 1, 0, 0, 0),
                                               datetime_end=datetime(2021, 2, 1, 23, 23, 59)).strftime(
        '%Y-%m-%d%H:%M:%S')

    id_hospital = random.randint(1, 21) if random.random() >= 0.15 else None
    brigade_code = random.randint(1, 51)

    return (customer, description, birthday, arrive_time,
            insurance_types, id_hospital, brigade_code,
            paramedic, paramedic2, nurse, driver, doctor, types)


def fake_data_gen(length, locale, seed) -> dict:
    """Generate fake data

    Args:
        length
        locale
        seed

    Returns:
        dict
                call_id
                first_name
                last_name
                address
                date_of_birth
                description
                arrive_time
                patient_code
                insurance_types
                id_hospital
                brigade_code
                paramedic
                paramedic2
                nurse
                doctor
                doctor2
    """
    fake = Faker(locale)

    if seed is not None:
        Faker.seed(seed)

    for ix in range(1, length + 1):
        customer, description, birthday, arrive_time, insurance_types, \
        id_hospital, brigade_code, paramedic, paramedic2, nurse, driver, doctor, types = prepared(fake)
        yield {
            "call_id": ix,
            **customer,
            "address": fake.street_address(),
            "date_of_birth": birthday,
            "description": description,
            "arrive_date": arrive_time[:10],
            "arrive_time": arrive_time[10:],
            "patient_code": random.randint(1, 5000),
            "insurance_types": insurance_types,
            "id_hospital": id_hospital,
            "brigade_code": brigade_code,
            "paramedic": paramedic[0],
            "paramedic_code": paramedic[1],
            "paramedic2": paramedic2[0],
            "paramedic2_code": paramedic2[1],
            "nurse": nurse[0],
            "nurse_code": nurse[1],
            "driver": driver[0],
            "driver_code": driver[1],
            "doctor": doctor[0],
            "doctor_code": doctor[1],
            "types": types
        }


def to_local_file(file_out: str, fields_dict: List, length: int = 10000, locale: str = "en_US", seed: int = 42):
    """
        Save rows to file
    Args:
        file_out:
        fields_dict:
        length:
        locale:
        seed:

    Returns:

    """
    time = datetime.now().strftime('%Y%m%d%H%M')
    with open(
            f"{str(Path(__file__).parent.parent)}/data_files/{file_out}{time}.csv",
            'w', newline='',
            encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fields_dict)
        writer.writeheader()
        for row in fake_data_gen(length, locale, seed):
            print(row)
            writer.writerow(row)
    return time


def load_to_aws(file_out: str, bucket: str, time: str):
    session = boto3.session.Session()
    client = session.client("s3")
    with open(
            f"{str(Path(__file__).parent.parent)}/data_files/{file_out}{time}.csv",
            'rb') as csvfile:
        client.put_object(
            Bucket=bucket,
            Key=f"data/{file_out}{time}.csv",
            Body=csvfile
        )


if __name__ == "__main__":
    fields = ["call_id", "first_name", "last_name", "address", "date_of_birth", "description",
              "arrive_date", "arrive_time", "patient_code", "insurance_types", "id_hospital", "brigade_code",
              "paramedic", "paramedic_code", "paramedic2", "paramedic2_code", "nurse", "nurse_code", "driver",
              "driver_code", "doctor", "doctor_code", "types"]
    time = to_local_file("out", fields, length=100000, locale='en_US', seed=42)
    # load_to_aws("out", "aws-warehouse", time)
