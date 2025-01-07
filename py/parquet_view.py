import streamlit as st
import pandas as pd

def load_parquet(file_path):
    try:
        df = pd.read_parquet(file_path)
        return df
    except Exception as e:
        st.error(f"无法读取 Parquet 文件: {e}")
        return None

def main():
    st.title("Parquet 文件查看器")

    # 输入 Parquet 文件路径
    file_path = st.text_input("请输入 Parquet 文件的路径", "")

    if file_path:
        df = load_parquet(file_path)
        if df is not None:
            st.success("Parquet 文件加载成功！")

            # 显示数据表
            st.dataframe(df)

            st.sidebar.header("筛选选项")

            # 动态生成筛选器
            filter_columns = st.sidebar.multiselect("选择要筛选的列", df.columns.tolist())

            filters = {}
            for col in filter_columns:
                unique_values = df[col].dropna().unique().tolist()
                selected_values = st.sidebar.multiselect(f"选择 {col} 的值", unique_values, default=unique_values)
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
            st.sidebar.header("排序选项")
            sort_column = st.sidebar.selectbox("选择排序的列", options=df.columns.tolist(), index=0)
            sort_order = st.sidebar.radio("排序顺序", options=["升序", "降序"])

            if sort_column:
                ascending = True if sort_order == "升序" else False
                sorted_df = filtered_df.sort_values(by=sort_column, ascending=ascending)
                st.write("排序后的数据：")
                st.dataframe(sorted_df)

if __name__ == "__main__":
    main()
