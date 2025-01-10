import random
import csv

column_values_pair = {
    'gender': ['Male', 'Female'],
    'SeniorCitizen': [0, 1],
    'Partner': ['Yes', 'No'],
    'Dependents': ['Yes', 'No'],
    'tenure': [0, 72],
    'PhoneService': ['Yes', 'No'],
    'MultipleLines': ['Yes', 'No', 'No phone service'],
    'InternetService': ['DSL', 'Fiber optic', 'No'],
    'OnlineSecurity': ['Yes', 'No', 'No internet service'],
    'OnlineBackup': ['Yes', 'No', 'No internet service'],
    'DeviceProtection': ['Yes', 'No', 'No internet service'],
    'TechSupport': ['Yes', 'No', 'No internet service'],
    'StreamingTV': ['Yes', 'No', 'No internet service'],
    'StreamingMovies': ['Yes', 'No', 'No internet service'],
    'Contract': ['Month-to-month', 'One year', 'Two year'],
    'PaperlessBilling': ['Yes', 'No'],
    'PaymentMethod': ['Bank transfer (automatic)', 'Credit card (automatic)', 'Electronic check', 'Mailed check'],
    'MonthlyCharges': [15, 125],
    'TotalCharges': [15, 1000],
    'Churn': ['Yes', 'No'],
}

NEW_FILE_PATH = '/shared/data/customer_churn_new.csv'

OLD_FILE_PATH = '/shared/data/customer_churn_old.csv'

def generate_biased_data(min_val, max_val, mid_val, is_int):
    std_dev = (max_val - min_val) / 6
    
    value = random.gauss(mid_val, std_dev)
    value = max(min_val, min(value, max_val))

    if is_int:
        return round(value)
    return round(value, 2)

def generate_normal_data():
    customer_id = f"{random.randint(1000, 9999)}{''.join(random.choices(string.ascii_uppercase, k=5))}"

    gender = random.choice(column_values_pair['gender'])

    senior_citizen = random.choice(column_values_pair['SeniorCitizen'])

    partner = random.choice(column_values_pair['Partner'])

    dependents = random.choice(column_values_pair['Dependents'])

    tenure = random.randrange(column_values_pair['tenure'][0], column_values_pair['tenure'][1]+1)

    phone_service = random.choice(column_values_pair['PhoneService'])
    if phone_service == 'Yes':
        multiple_lines = random.choice(column_values_pair['MultipleLines'])
    else:
        multiple_lines = column_values_pair['MultipleLines'][-1]

    internet_service = random.choice(column_values_pair['InternetService'])
    if internet_service == 'No':
        online_security =  column_values_pair['OnlineSecurity'][-1]
        online_backup = column_values_pair['OnlineBackup'][-1]
        device_protection = column_values_pair['DeviceProtection'][-1]
        tech_support = column_values_pair['TechSupport'][-1]
        streaming_tv = column_values_pair['StreamingTV'][-1]
        streaming_movies = column_values_pair['StreamingMovies'][-1]
    else:
        online_security = random.choice(column_values_pair['OnlineSecurity'])
        online_backup = random.choice(column_values_pair['OnlineBackup'])
        device_protection = random.choice(column_values_pair['DeviceProtection'])
        tech_support = random.choice(column_values_pair['TechSupport'])
        streaming_tv = random.choice(column_values_pair['StreamingTV'])
        streaming_movies = random.choice(column_values_pair['StreamingMovies'])

    contract = random.choice(column_values_pair['Contract'])

    paperless_billing = random.choice(column_values_pair['PaperlessBilling'])

    payment_method = random.choice(column_values_pair['PaymentMethod'])

    monthly_charges = random.randrange(column_values_pair['MonthlyCharges'][0], column_values_pair['MonthlyCharges'][1] + 1)
    
    total_charges = random.randrange(column_values_pair['TotalCharges'][0], column_values_pair['TotalCharges'][1] + 1)

    churn = random.choice(column_values_pair['Churn'])

    return [
        customer_id, gender, senior_citizen, partner, dependents, tenure, phone_service, multiple_lines, internet_service, 
        online_security, online_backup, device_protection, tech_support, streaming_tv, streaming_movies, contract, 
        paperless_billing, payment_method, monthly_charges, total_charges, churn
    ]

def drift_simulator(data_count):
    random_column = random.choice(column_values_pair.keys()[:-1])

    data = []

    while len(data) < data_count:
        normal = generate_normal_data()

        if random_column == 'tenure':
            min_val = column_values_pair['tenure'][0]
            max_val = column_values_pair['tenure'][1]
            mid_val = random.randrange(min_val, max_val + 1)

            normal[5] = generate_biased_data(min_val, max_val, mid_val, True)

        elif random_column == 'MonthlyCharges':
            min_val = column_values_pair['MonthlyCharges'][0]
            max_val = column_values_pair['MonthlyCharges'][1]
            mid_val = random.randrange(min_val, max_val + 1)

            normal[18] = generate_biased_data(min_val, max_val, mid_val, False)
        elif random_column == 'TotalCharges':
            min_val = column_values_pair['TotalCharges'][0]
            max_val = column_values_pair['TotalCharges'][1]
            mid_val = random.randrange(min_val, max_val + 1)

            normal[19] = generate_biased_data(min_val, max_val, mid_val, False)

        data.append(normal)

    if os.path.exists(NEW_FILE_PATH):
        with open(NEW_FILE_PATH, 'r') as f:
            f.readline()

            lines = f.readlines()
        
        with open(OLD_FILE_PATH, 'a') as f:
            f.write('\n')
            f.write(lines)
    
    with open(NEW_FILE_PATH, 'w', newline='') as f:
        writer = csv.writer(f)

        writer.writerow(['customerID'] + list(column_values_pair.keys()))

        writer.writerows(data)

    