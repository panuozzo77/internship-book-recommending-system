# analyze_results.py
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import ast
import io

def analyze_performance_data(filename="performance_results_llm.csv"):
    try:
        df = pd.read_csv(filename)
    except FileNotFoundError:
        print(f"File '{filename}' non trovato. Esegui prima i test.")
        return

    # Convert field_sources from string to dict
    df['field_sources'] = df['field_sources'].apply(ast.literal_eval)

    print("--- Statistiche Descrittive Generali ---")
    print(df['execution_time_sec'].describe())
    print("\n")

    print("--- Riepilogo per Tipo di Test e Stato ---")
    summary = df.groupby(['test_type', 'status'])['execution_time_sec'].agg(['count', 'mean', 'std', 'min', 'max'])
    print(summary)
    print("\n")

    # --- Analisi Contributo Provider ---
    print("--- Contributo dei Provider per Campo ---")
    provider_contributions = df['field_sources'].apply(pd.Series).stack().reset_index(level=1)
    provider_contributions.columns = ['field', 'provider']
    
    contribution_summary = provider_contributions.groupby(['field', 'provider']).size().unstack(fill_value=0)
    print(contribution_summary)
    print("\n")

    plot_buffers = []  # Lista per salvare le immagini in memoria

    # --- Visualizzazioni ---
    sns.set_theme(style="whitegrid")

    # 1. Boxplot dei tempi di esecuzione per tipo di test
    plt.figure(figsize=(10, 6))
    sns.boxplot(x='test_type', y='execution_time_sec', data=df)
    plt.title('Distribuzione dei Tempi di Esecuzione per Tipo di Operazione')
    plt.xlabel('Tipo di Test')
    plt.ylabel('Tempo di Esecuzione (secondi)')
    
    buf1 = io.BytesIO()
    plt.savefig(buf1, format='png')
    buf1.seek(0)
    plot_buffers.append(buf1)
    plt.close()

    # 2. Bar chart del conteggio degli stati
    plt.figure(figsize=(12, 6))
    sns.countplot(y='status', data=df, order=df['status'].value_counts().index)
    plt.title('Conteggio dei Risultati per Stato')
    plt.xlabel('Numero di Occorrenze')
    plt.ylabel('Stato del Risultato')
    plt.tight_layout()
    #plt.show()

    buf2 = io.BytesIO()
    plt.savefig(buf2, format='png')
    buf2.seek(0)
    plot_buffers.append(buf2)
    #plt.close()

    # 3. Grafico contributo provider
    contribution_summary.plot(kind='bar', stacked=True, figsize=(14, 8))
    plt.title('Contributo dei Provider per Campo')
    plt.xlabel('Campo Metadati')
    plt.ylabel('Numero di Libri')
    plt.xticks(rotation=45, ha='right')
    plt.legend(title='Provider')
    plt.tight_layout()
    #plt.show()

    buf3 = io.BytesIO()
    plt.savefig(buf3, format='png')
    buf3.seek(0)
    plot_buffers.append(buf3)
    plt.close()

    print(f"{len(plot_buffers)} grafici salvati in memoria.")

    return plot_buffers

if __name__ == '__main__':
    # Cambia il nome del file a seconda di quale test hai eseguito
    array = analyze_performance_data("performance_results_llm.csv")
    # analyze_performance_data("performance_results_keyword.csv")
    for i in range(1, 4):
        with open(f"grafico_{i}.png", 'wb') as f:
            f.write(array[i-1].getvalue())