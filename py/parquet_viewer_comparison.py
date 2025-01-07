import streamlit as st
import pandas as pd
from typing import List, Dict

def load_parquet(file_path: str) -> pd.DataFrame:
    try:
        df = pd.read_parquet(file_path)
        return df
    except Exception as e:
        st.error(f"无法读取 Parquet 文件 `{file_path}`: {e}")
        return None

def compare_dataframes(dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    比较多个 DataFrame 之间的差异。
    假设所有 DataFrame 具有相同的列和索引。
    返回一个包含差异信息的 DataFrame。
    """
    if not dfs:
        return pd.DataFrame()

    # 获取所有 DataFrame 的集合
    keys = list(dfs.keys())
    base_key = keys[0]
    base_df = dfs[base_key]

    comparison_df = base_df.copy()
    comparison_df['Differences'] = ''

    for key in keys[1:]:
        df = dfs[key]
        diff = base_df != df
        comparison_df['Differences'] += diff.apply(lambda row: ', '.join(df.columns[row.values]) if row.any() else '', axis=1)

    # 去除空的差异
    comparison_df = comparison_df[comparison_df['Differences'] != '']

    return comparison_df

def main():
    st.title("多 Parquet 文件查看与对比工具")

    st.sidebar.header("输入 Parquet 文件路径")
    file_paths_input = st.sidebar.text_area(
        "请输入 Parquet 文件的路径，每行一个路径",
        "path/to/file1.parquet\npath/to/file2.parquet"
    )

    file_paths = [path.strip() for path in file_paths_input.strip().split('\n') if path.strip()]
    loaded_dfs = {}
    for path in file_paths:
        df = load_parquet(path)
        if df is not None:
            loaded_dfs[path] = df

    if loaded_dfs:
        st.success(f"成功加载 {len(loaded_dfs)} 个 Parquet 文件。")
        
        # 选择要查看的文件
        selected_files = st.multiselect(
            "选择要查看的文件",
            options=list(loaded_dfs.keys()),
            default=list(loaded_dfs.keys())
        )

        for file in selected_files:
            st.subheader(f"文件: {file}")
            df = loaded_dfs[file]

            # 显示数据表
            st.dataframe(df)

            # 筛选和排序功能
            st.markdown(f"### 筛选和排序 - {file}")
            with st.expander(f"筛选和排序选项 - {file}"):
                # 动态生成筛选器
                filter_columns = st.multiselect(f"选择要筛选的列 - {file}", df.columns.tolist())

                filters = {}
                for col in filter_columns:
                    unique_values = df[col].dropna().unique().tolist()
                    selected_values = st.multiselect(f"选择 `{col}` 的值", unique_values, default=unique_values, key=f"{file}_{col}")
                    if selected_values:
                        filters[col] = selected_values

                # 应用筛选
                if filters:
                    filtered_df = df.copy()
                    for col, values in filters.items():
                        filtered_df = filtered_df[filtered_df[col].isin(values)]
                    st.write("筛选后的数据：")
                    st.dataframe(filtered_df)
                else:
                    filtered_df = df.copy()

                # 排序
                sort_column = st.selectbox(f"选择排序的列 - {file}", options=df.columns.tolist(), key=f"sort_{file}")
                sort_order = st.radio(f"排序顺序 - {file}", options=["升序", "降序"], key=f"order_{file}")

                if sort_column:
                    ascending = True if sort_order == "升序" else False
                    sorted_df = filtered_df.sort_values(by=sort_column, ascending=ascending)
                    st.write("排序后的数据：")
                    st.dataframe(sorted_df)

        # 比较功能
        if len(loaded_dfs) > 1:
            st.header("对比不同文件之间的差异")
            selected_compare_files = st.multiselect(
                "选择要对比的文件（至少选择两个）",
                options=list(loaded_dfs.keys())
            )

            if len(selected_compare_files) >= 2:
                # 准备要对比的 DataFrames
                compare_dfs = {file: loaded_dfs[file] for file in selected_compare_files}

                # 检查所有 DataFrame 是否具有相同的列
                columns_set = [set(df.columns) for df in compare_dfs.values()]
                if len(set(map(tuple, columns_set))) > 1:
                    st.error("所选文件的列不完全相同，无法进行对比。")
                else:
                    # 检查是否具有相同的索引
                    indices_set = [set(df.index) for df in compare_dfs.values()]
                    if len(set(map(tuple, indices_set))) > 1:
                        st.warning("所选文件的索引不完全相同，比较结果可能不准确。")

                    diff_df = compare_dataframes(compare_dfs)
                    if not diff_df.empty:
                        st.write("不同文件之间的差异：")
                        st.dataframe(diff_df)
                    else:
                        st.success("所选文件之间没有差异。")
            else:
                st.info("请选择至少两个文件进行对比。")

    else:
        st.warning("请在侧边栏输入至少一个有效的 Parquet 文件路径。")

if __name__ == "__main__":
    main()

