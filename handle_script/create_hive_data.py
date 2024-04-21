import json
import csv

# 读取JSON数据
with open('CleanData1.json', 'r', encoding='utf-8') as json_file:
    data = json.load(json_file)

# 准备要写入CSV的字段
fields = [
    'modelId', 'lastModified', 'pipeline_tag', 'author',
    'id', 'likes', 'downloads', 'library_name', 'datasets', 'architectures'
]

# 打开CSV文件准备写入
with open('hive_data.csv', 'w', encoding='utf-8-sig', newline='') as csv_file:
    writer = csv.DictWriter(csv_file, fieldnames=fields)

    # 写入表头
    writer.writeheader()
    # 遍历JSON数据中的每个模型
    for model in data:
        # 假设 model 是一个可能为 None 或字典的对象
        datasets_str = 'N/A'
        if isinstance(model, dict) and 'cardData' in model and isinstance(model['cardData'], dict):
            datasets_list = model['cardData'].get('datasets', [])
            if datasets_list:  # 确保列表不为空
                datasets_str = ','.join(map(str, datasets_list))  # 将列表中的元素转换为字符串并连接

        architectures_str = 'N/A'
        if isinstance(model, dict) and 'config' in model and isinstance(model['config'], dict):
            architectures_list = model['config'].get('architectures', [])
            if architectures_list:  # 确保列表不为空
                architectures_str = ','.join(map(str, architectures_list))  # 将列表中的元素转换为字符串并连接
        # 提取模型的相关信息
        model_info = {
            'modelId': model.get('modelId', 'N/A'),
            'lastModified': model.get('lastModified', 'N/A'),
            'pipeline_tag': model.get('pipeline_tag', 'N/A'),
            'author': model.get('author', 'unknown'),  # 如果没有author字段，使用'N/A'代替
            'id': model.get('_id', 'N/A'),  # 使用_id字段
            'likes': model.get('likes', 'N/A'),
            'downloads': model.get('downloads', 'N/A'),
            'library_name': model.get('library_name', 'N/A'),
            # 添加datasets字段，并将其转换为字符串
            # 'datasets': ','.join(model.get('cardData', {}).get('datasets', [])) or 'N/A',
            # 'architectures': ','.join(model.get('config', {}).get('architectures', [])) or 'N/A'
            'datasets': datasets_str,
            'architectures': architectures_str
        }

        # 写入模型信息到CSV文件
        writer.writerow(model_info)

print("CSV文件已生成。")