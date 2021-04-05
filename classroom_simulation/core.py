import honeycomb_io
import pandas as pd
import tqdm
import datetime
import dateutil
import random
import uuid
import logging

logger = logging.getLogger(__name__)

def generate_interaction_data_day(
    target_date,
    time_zone_name,
    student_person_ids,
    material_id_lookup,
    start_hour = 8,
    end_hour = 16,
    idle_duration_minutes=20,
    tray_carry_duration_seconds=10,
    material_usage_duration_minutes=40,
    step_size_seconds=0.1
):
    target_date = pd.to_datetime(target_date).date()
    day_start = datetime.datetime(
        target_date.year,
        target_date.month,
        target_date.day,
        start_hour,
        tzinfo=dateutil.tz.gettz(time_zone_name)
    )
    day_end = datetime.datetime(
        target_date.year,
        target_date.month,
        target_date.day,
        end_hour,
        tzinfo=dateutil.tz.gettz(time_zone_name)
    )
    tray_interactions, material_interactions = generate_interaction_data(
        start=day_start,
        end=day_end,
        student_person_ids=student_person_ids,
        material_id_lookup=material_id_lookup,
        idle_duration_minutes=idle_duration_minutes,
        tray_carry_duration_seconds=tray_carry_duration_seconds,
        material_usage_duration_minutes=material_usage_duration_minutes,
        step_size_seconds=step_size_seconds
    )
    return tray_interactions, material_interactions

def generate_interaction_data(
    start,
    end,
    student_person_ids,
    material_id_lookup,
    idle_duration_minutes=20,
    tray_carry_duration_seconds=10,
    material_usage_duration_minutes=40,
    step_size_seconds=0.1
):
    logger.info('Generating data from {} to {}'.format(start, end))
    logger.info('Generating data for {} students'.format(len(student_person_ids)))
    tray_ids = list(material_id_lookup.keys())
    material_ids = list(material_id_lookup.values())
    tray_id_lookup = {material_id: tray_id for tray_id, material_id in material_id_lookup.items()}
    logger.info('Generating data for {} trays/materials'.format(len(tray_ids)))
    # Initial student states
    student_states = dict()
    for student_person_id in student_person_ids:
        student_states[student_person_id] = {
            'state': 'idle',
            'tray_interaction_id': None,
            'material_interation_id': None
        }
    # Initialize tray states
    tray_states = dict()
    for tray_id in tray_ids:
        tray_states[tray_id] = 'on_shelf'
    # Generate data
    num_steps = round((end - start).total_seconds()/step_size_seconds)
    time_series = dict()
    tray_interactions = dict()
    material_interactions = dict()
    for step_index in tqdm.tqdm(range(num_steps)):
        timestamp = start + datetime.timedelta(seconds = step_index*step_size_seconds)
        for student_person_id in student_states.keys():
            if student_states[student_person_id]['state'] == 'idle':
                if random.random() > step_size_seconds/(idle_duration_minutes*60):
                    continue
                available_tray_ids = list(filter(
                    lambda tray_id: tray_states[tray_id] == 'on_shelf',
                    tray_states.keys()
                ))
                if len(available_tray_ids) == 0:
                    continue
                selected_tray_id = random.choice(available_tray_ids)
                tray_interaction_id = str(uuid.uuid4())
                tray_interactions[tray_interaction_id] = {
                    'person': student_person_id,
                    'tray': selected_tray_id,
                    'start': timestamp,
                    'interaction_type': 'CARRYING_FROM_SHELF'
                }
                student_states[student_person_id]['state'] = 'carrying_from_shelf'
                student_states[student_person_id]['tray_interaction_id'] = tray_interaction_id
                tray_states[selected_tray_id] = 'carrying_from_shelf'
            elif student_states[student_person_id]['state'] == 'carrying_from_shelf':
                if random.random() > step_size_seconds/tray_carry_duration_seconds:
                    continue
                tray_interaction_id = student_states[student_person_id]['tray_interaction_id']
                tray_id = tray_interactions[tray_interaction_id]['tray']
                material_id = material_id_lookup[tray_id]
                material_interaction_id = str(uuid.uuid4())
                material_interactions[material_interaction_id] = {
                    'person': student_person_id,
                    'material': material_id,
                    'start': timestamp
                }
                tray_interactions[tray_interaction_id]['end'] = timestamp
                student_states[student_person_id]['state'] = 'using_material'
                student_states[student_person_id]['tray_interaction_id'] = None
                student_states[student_person_id]['material_interaction_id'] = material_interaction_id
                tray_states[tray_id] = 'using_material'
            elif student_states[student_person_id]['state'] == 'using_material':
                if random.random() > step_size_seconds/(material_usage_duration_minutes*60):
                    continue
                material_interaction_id = student_states[student_person_id]['material_interaction_id']
                material_id = material_interactions[material_interaction_id]['material']
                tray_id = tray_id_lookup[material_id]
                tray_interaction_id = str(uuid.uuid4())
                tray_interactions[tray_interaction_id] = {
                    'person': student_person_id,
                    'tray': tray_id,
                    'start': timestamp,
                    'interaction_type': 'CARRYING_TO_SHELF'
                }
                material_interactions[material_interaction_id]['end'] = timestamp
                student_states[student_person_id]['state'] = 'carrying_to_shelf'
                student_states[student_person_id]['tray_interaction_id'] = tray_interaction_id
                student_states[student_person_id]['material_interaction_id'] = None
                tray_states[tray_id] = 'carrying_to_shelf'
            elif student_states[student_person_id]['state'] == 'carrying_to_shelf':
                if random.random() > step_size_seconds/tray_carry_duration_seconds:
                    continue
                tray_interaction_id = student_states[student_person_id]['tray_interaction_id']
                tray_id = tray_interactions[tray_interaction_id]['tray']
                tray_interactions[tray_interaction_id]['end'] = timestamp
                student_states[student_person_id]['state'] = 'idle'
                student_states[student_person_id]['tray_interaction_id'] = None
                tray_states[tray_id] = 'on_shelf'
            else:
                raise ValueError('Student state \'{}\' not recognized'.format(
                    student_states[student_person_id]['state']
                ))
    return tray_interactions, material_interactions
