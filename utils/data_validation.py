import re
import pandas as pd
from datetime import datetime
import logging

# Regex pré-compiladas
EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
PLATE_OLD_REGEX = re.compile(r"^[A-Z]{3}\d{4}$", re.IGNORECASE)
PLATE_MERCOSUL_REGEX = re.compile(r"^[A-Z]{3}\d[A-Z]\d{2}$", re.IGNORECASE)
YEAR_REGEX = re.compile(r"^\d{4}$")

# Mensagens de erro centralizadas
ERROR_MESSAGES = {
    "Obrigatorio": "Campo '{col}': obrigatório e não preenchido. Valor atual: '{valor}'",
    "ObrigatorioCondicional": "Campo '{col}': obrigatório e não preenchido, pois '{condicao_campo}' está preenchido. Valor atual: '{valor}'",
    "Data": "Campo '{col}': formato de data inválido ou data irreal (esperado AAAA-MM-DD). Valor: '{valor}'",
    "Numerico": "Campo '{col}': valor não numérico (use ponto ou vírgula para decimais). Valor: '{valor}'",
    "Inteiro": "Campo '{col}': valor não é um número inteiro válido. Valor: '{valor}'",
    "Email": "Campo '{col}': formato de e-mail inválido. Valor: '{valor}'",
    "CPF_CNPJ": "Campo '{col}': CPF/CNPJ inválido (deve ter 11 ou 14 dígitos numéricos). Valor: '{valor}'",
    "CEP": "Campo '{col}': CEP inválido (deve ter 8 dígitos numéricos). Valor: '{valor}'",
    "Ano": "Campo '{col}': ano inválido (esperado 4 dígitos numéricos). Valor: '{valor}'",
    "SimNao": "Campo '{col}': valor inválido (esperado '0' para Não ou '1' para Sim). Valor: '{valor}'",
    "FaixaRenda": "Campo '{col}': faixa de renda inválida (esperado '1', '2' ou '3'). Valor: '{valor}'",
    "Placa": "Campo '{col}': formato de placa inválido (ex: ABC1234 ou ABC1D23). Valor: '{valor}'",
    "VEICULO_NOVO_ValoresPermitidos": "Campo 'VEICULO_NOVO': valor inválido (esperado 'N' para Novo ou 'U' para Usado). Valor: '{valor}'",
    "ValoresPermitidos": "Campo '{col}': valor inválido (permitido apenas: {permitidos}). Valor: '{valor}'",
}


class DataValidator:
    """
    Classe estática para agrupar funções de validação de dados.
    Fornece métodos para validar diferentes tipos de dados com mensagens específicas.
    """

    @staticmethod
    def is_valid_date(value):
        """Valida se a data está no formato AAAA-MM-DD e se é uma data real."""
        if not isinstance(value, str):
            return False
        try:
            datetime.strptime(value, "%Y-%m-%d")
            return True
        except ValueError:
            return False

    @staticmethod
    def is_numeric(value):
        """Valida se o valor pode ser convertido para float (numérico)."""
        if not isinstance(value, str):
            return False
        try:
            float(value.replace(",", "."))
            return True
        except ValueError:
            return False

    @staticmethod
    def is_integer(value):
        """Valida se o valor pode ser convertido para inteiro."""
        if not isinstance(value, str):
            return False
        try:
            int(float(value.replace(",", ".")))
            return True
        except ValueError:
            return False

    @staticmethod
    def is_valid_email(value):
        """Valida o formato de um endereço de e-mail."""
        if not isinstance(value, str):
            return False
        return EMAIL_REGEX.match(value) is not None

    @staticmethod
    def is_valid_cpf_cnpj(value):
        """Valida o formato de CPF (11 dígitos) ou CNPJ (14 dígitos)."""
        if not isinstance(value, str):
            return False
        cleaned_value = re.sub(r"\D", "", value)
        if len(cleaned_value) == 11 and cleaned_value.isdigit():
            return True
        elif len(cleaned_value) == 14 and cleaned_value.isdigit():
            return True
        return False

    @staticmethod
    def is_valid_cep(value):
        """Valida o formato de CEP (8 dígitos numéricos)."""
        if not isinstance(value, str):
            return False
        cleaned_value = re.sub(r"\D", "", value)
        return len(cleaned_value) == 8 and cleaned_value.isdigit()

    @staticmethod
    def is_valid_year(value):
        """Valida se o valor é um ano de 4 dígitos."""
        if not isinstance(value, str):
            return False
        return YEAR_REGEX.match(value) is not None and value.isdigit()

    @staticmethod
    def is_sim_nao(value):
        """Valida se o valor é '0' (Não) ou '1' (Sim)."""
        if not isinstance(value, str):
            return False
        return value in ["0", "1"]

    @staticmethod
    def is_faixa_renda(value):
        """Valida se o valor corresponde aos códigos de faixa de renda (1, 2, 3)."""
        if not isinstance(value, str):
            return False
        return value in ["1", "2", "3"]

    @staticmethod
    def is_valid_plate(value):
        """Valida formatos de placa (Mercosul e Antigo)."""
        if not isinstance(value, str):
            return False
        return (
            PLATE_OLD_REGEX.match(value) is not None
            or PLATE_MERCOSUL_REGEX.match(value) is not None
        )


# Dicionário de tipos para funções de validação
TIPO_VALIDADORES = {
    "Data": DataValidator.is_valid_date,
    "DataCondicional": DataValidator.is_valid_date,
    "Numerico": DataValidator.is_numeric,
    "Inteiro": DataValidator.is_integer,
    "Email": DataValidator.is_valid_email,
    "CPF_CNPJ": DataValidator.is_valid_cpf_cnpj,
    "CEP": DataValidator.is_valid_cep,
    "Ano": DataValidator.is_valid_year,
    "SimNao": DataValidator.is_sim_nao,
    "FaixaRenda": DataValidator.is_faixa_renda,
    "Placa": DataValidator.is_valid_plate,
}


def validar_dados(row, layout_rules, validar_nao_obrigatorios_flag):
    erros = []
    for col, regras in layout_rules.items():
        valor = str(row.get(col, "")).strip()

        # Verifica se o campo é obrigatório e está vazio
        if regras["Obrigatorio"] and (
            valor == "" or pd.isna(row.get(col)) or valor.lower() == "nan"
        ):
            if regras.get("Tipo") == "DataCondicional":
                condicao_campo = regras.get("CondicaoCampo")
                valor_condicao = str(row.get(condicao_campo, "")).strip()
                if regras.get("CondicaoValor") == "NAO_VAZIO" and (
                    valor_condicao != ""
                    and not pd.isna(row.get(condicao_campo))
                    and valor_condicao.lower() != "nan"
                ):
                    erros.append(
                        ERROR_MESSAGES["ObrigatorioCondicional"].format(
                            col=col, condicao_campo=condicao_campo, valor=valor
                        )
                    )
            else:
                erros.append(ERROR_MESSAGES["Obrigatorio"].format(col=col, valor=valor))
            logging.debug(
                f"Erro em {col}: obrigatório não preenchido. Valor: '{valor}'"
            )
            continue

        if (not regras["Obrigatorio"] and not validar_nao_obrigatorios_flag) or (
            valor == "" or pd.isna(row.get(col)) or valor.lower() == "nan"
        ):
            continue

        # Validação de valores permitidos (prioridade para VEICULO_NOVO)
        if (
            "ValoresPermitidos" in regras
            and valor not in regras["ValoresPermitidos"]
            and valor != ""
        ):
            permitidos = ", ".join(regras["ValoresPermitidos"])
            if col == "VEICULO_NOVO":
                erros.append(
                    ERROR_MESSAGES["VEICULO_NOVO_ValoresPermitidos"].format(valor=valor)
                )
            else:
                erros.append(
                    ERROR_MESSAGES["ValoresPermitidos"].format(
                        col=col, valor=valor, permitidos=permitidos
                    )
                )
            continue  # Não faz validação de tipo se já caiu aqui

        tipo = regras.get("Tipo")
        # NUNCA valide tipo para VEICULO_NOVO, pois só deve validar os valores permitidos
        if col != "VEICULO_NOVO" and tipo:
            validador = TIPO_VALIDADORES.get(tipo)
            if validador and not validador(valor):
                erros.append(
                    ERROR_MESSAGES.get(
                        tipo, f"Campo '{col}': valor inválido. Valor: '{valor}'"
                    ).format(col=col, valor=valor)
                )

    return erros
