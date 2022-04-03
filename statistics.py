poo = {i: 0 for i in range(len(Position.POSITIONS))}
print(ANNO)
for p in people:
    poo[p.pos.position] += 1

for i in poo:
    print(Position.POSITIONS[i], poo[i])

print('=' * 20)

talcount = {i: 0 for i in range(TALENT_MIN, TALENT_MAX + 1)}
talsum = {i: 0 for i in range(TALENT_MIN, TALENT_MAX + 1)}
for p in people:
    talcount[p.talent] += 1
    talsum[p.talent] += p.pos.position

print('Среднее значение должности в зависмомти от таланта')
talmean = dict()
for c in talcount:
    if talcount[c] == 0:
        talmean[c] = 0
    else:
        talmean[c] = talsum[c] / talcount[c]
    print(f'талант:{c}, людей {talcount[c]} средняя должность {talmean[c]}')

limits = {0: 20, 1: 30, 2: 40, 3: 50, 4: 60, 5: 1000}


def pockets(birth):
    ret = None
    age = (ANNO - birth).days / 365
    for i in limits:
        if age < limits[i]:
            ret = i
            break
    return i


agcount = {i: 0 for i in range(len(limits))}
agsum = {i: 0 for i in range(len(limits))}
for p in people:
    agcount[pockets(p.age)] += 1
    agsum[pockets(p.age)] += p.pos.position

print('=' * 20)
print('Среднее значение должности в зависмомти от возраста')
agmean = dict()
for c in agcount:
    if agcount[c] == 0:
        agmean[c] = 0
    else:
        agmean[c] = agsum[c] / agcount[c]
    print(f'возраст до:{limits[c]}, людей {agcount[c]} средняя должность {agmean[c]}')

# вычисляем количество людей в каждой фирме
firms_count = {i: 0 for i in firms_list}
for p in people:
    firms_count[p.firm] += 1

print('=' * 20)
for f in firms_count:
    print(f'{f.name}  рпестиж: {f.attraction}   сотрудников: {firms_count[f]}')

# просто выборка из несеольких людей
print('=' * 20)
for i in people[:16]:
    print(i)

# выборка имеющихся должностей
print('=' * 20)
psd = session.query(distinct(Human.pos_id)).order_by(Human.pos_id).all()
print(psd)

# РАСПРЕДЕЛЕНИЕ ЛЮДЕЙ ПО ДОЛЖНОСТЯМ
# первый способ
print('*' * 40, '/nпервый способ')
x = session.query(func.count(Human.id).label('cont'), PosBase.name).join(PosBase).group_by(Human.pos_id).order_by(
    Human.pos_id).all()
for y in x:
    print(y.cont, y.name)

# второй способ
print('*' * 40, '/nвторой способ')
for i in psd:
    x = session.query(func.count(Human.id))
    x = x.filter(Human.pos_id == i[0])
    x = x.scalar()
    print(session.query(PosBase.name).filter(PosBase.id == i[0]).scalar(), x)
