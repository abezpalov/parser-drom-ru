import requests
import lxml.html

from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool

# Настройки
threads = 16  # Количество потоков
log = 0  # Уровень детализации вывода процесса на экран


def get_region_list() -> list:
    """
    Получает список регионов и генерирует ссылки на объявления в них

    Возвращает список регионов в формате:
    [{'name': str,
      'url': str,},
      ...
    ]
    """

    # Инициализируем переменные
    regions = []

    # Получаем дерево HTML с регионами для парсига
    s = requests.Session()
    result = s.get('https://www.drom.ru/my_region/')
    tree = lxml.html.fromstring(result.text)

    # Получаем все ссылки на смену региона
    links = tree.xpath(f'.//a[contains(@href, "https://www.drom.ru/my_region/?set_region=")]')

    # Из списка ссылок смены региона формируем список регионов с ссылками на объявления в них
    for link in links:
        name = link.text
        number = link.attrib['href'].split('=')[-1]
        url = f'https://auto.drom.ru/region{number}/all/'
        region = {'name': name, 'url': url}
        print(f'Добавлен регион: {region}')

        regions.append(region)

    return regions


def parse_region(region: dict):
    """
    Парсит заданный регион.

    Входные данные:
    region = {'name': str,
              'url': str,}
    """

    # Оповещение о старте
    print(f'Начинаю обрабатывать регион: {region["name"]}')

    # Инициализация переменных
    urls_all = set()
    urls_visited = set()

    # Добавляем первую ссылку
    urls_all.add(region['url'])

    # Проходим по ссылкам пока все они не будут просмотренны
    while len(urls_all) > len(urls_visited):

        # Открываю сессию
        s = requests.Session()

        # Проходим по всем найденным на данным момент ссылкам
        for url in list(urls_all):

            # Если ссылку уже просмотрели - пропускаем
            if url in urls_visited:
                continue

            # Загружаем очередную страницу
            if log >= 1:
                print(f'Загружаю: {url}')
            result = s.get(url)
            tree = lxml.html.fromstring(result.text)

            # Получаем новые ссылки из пагинации и добавляем их в набор ссылок к просмотру
            links = tree.xpath(f'.//a[contains(@href, "{region["url"]}page")]/@href')
            for link in links:
                urls_all.add(link)

            # Получаем объявления
            ads = tree.xpath(f'.//a[contains(@data-ftid, "bulls-list_bull")]')
            for ad in ads:
                parce_ad(ad)

            urls_visited.add(url)

    # Оповещение о завершении
    print(f'Завершил обрабатывать регион: {region["name"]}')


def parce_ad(item):
    """
    Разбирает данные объявления для занесения в базу данных.
    """

    # Получаем ссылку
    link = item.attrib['href']

    # Получаем заголовок
    try:
        title = item.xpath('.//span[contains(@data-ftid, "_title")]')[0].text
    except IndexError:
        return None

    # Выделяем из заголовка модель и год
    model = title.split(',')[0].strip()
    year = int(title.split(',')[1].strip())

    # Выявляем промо
    try:
        promo = item.xpath('.//*[contains(@data-ftid, "_promotion")]/@title')[0]
    except IndexError:
        promo = None

    # Выявляем "битая"
    try:
        _ = item.xpath('.//*[contains(@data-ftid, "_broken")]/@title')[0]
        broken = True
    except IndexError:
        broken = False

    # Выделяем "от собственника"
    try:
        _ = item.xpath('.//*[contains(@data-ftid, "_owner")]/@title')[0]
        owner = True
    except IndexError:
        owner = False

    # Получаем локацию
    try:
        location = item.xpath('.//span[contains(@data-ftid, "_location")]')[0].text
    except IndexError:
        return None

    # Получаем и чистим цену
    price = int(item.xpath('.//*[contains(@data-ftid, "_price")]')[0].text.replace('\xa0', ''))

    if log >= 2:
        print(f'{title=}, {model=}, {year=}, {price=}, {promo=}, {broken=}, {owner=}, {location=}, {link=}')

    update_ad_data(title=title, model=model, year=year, price=price, promo=promo, broken=broken, owner=owner,
                   location=location, link=link)


def update_ad_data(title: str, model: str, year: int, price: int, promo: bool, broken: bool, owner: bool,
                   location: str, link: str):
    """
    Записывает данные в базу: если новое объявление - добавляет, если старое - обновляет
    """

    # TODO

    # Попытаться получить объявление с базы по уникальной ссылке
    pass

    # Если есть - обновить
    if True:
        pass
    # Если нет -
    else:
        pass


def main():
    regions = get_region_list()

    if threads:
        pool = ThreadPool(threads)
        pool.map(parse_region, regions)
    else:
        for region in regions:
            parse_region(region)


if __name__ == "__main__":
    main()
