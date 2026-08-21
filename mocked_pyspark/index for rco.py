<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">

    <title>Rule Group Builder</title>

    <link
        href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css"
        rel="stylesheet"
    >

    <style>
        body {
            background: #f5f6f8;
            padding: 40px;
        }

        .rule-builder {
            max-width: 1000px;
            margin: auto;
        }

        .group {
            border: 1px solid #d9dce1;
            border-radius: 8px;
            background: #fff;
            margin-bottom: 12px;
            overflow: hidden;
        }

        .group-header {
            display: flex;
            align-items: center;
            gap: 10px;
            padding: 10px 12px;
            background: #f8f9fa;
            border-bottom: 1px solid #e1e3e6;
        }

        .group-children {
            padding: 12px 12px 4px 40px;
            position: relative;
        }

        .group-children::before {
            content: "";
            position: absolute;
            left: 20px;
            top: 0;
            bottom: 20px;
            border-left: 2px solid #e1e3e6;
        }

        .rule-row {
            display: flex;
            align-items: center;
            gap: 8px;
            margin-bottom: 10px;
            position: relative;
        }

        .rule-row::before {
            content: "";
            position: absolute;
            left: -20px;
            top: 15px;
            width: 20px;
            border-top: 2px solid #e1e3e6;
        }

        .logic {
            width: 80px;
            flex-shrink: 0;
        }

        .rule-content {
            flex: 1;
            display: flex;
            align-items: flex-start;
            gap: 8px;
        }

        .rule-content select {
            flex: 0.3;
        }

        .rule-parameters {
            display: grid;
            flex: 0.7;
            grid-template-columns: 1fr 1fr;
            gap: 8px;
        }

        .group-content {
            flex: 1;
        }

        .group-title {
            font-weight: 600;
            font-size: 14px;
        }

        .actions {
            display: flex;
            gap: 6px;
        }

        .add-actions {
            display: flex;
            gap: 8px;
            padding: 4px 0 8px;
        }

        .btn-add {
            font-size: 13px;
        }

        .root {
            border: 2px solid #bfc5cd;
        }

        .root > .group-header {
            background: #eef1f5;
        }
    </style>
</head>

<body>

<div class="rule-builder">

    <h4 class="mb-4">Rule Builder</h4>

    <div id="ruleBuilder"></div>

</div>


<script>

let groupId = 0;
let conditionId = 0;


/* =========================================================
   DATA
   ========================================================= */


const rules = [
    {
        "id": "pix_envio",
        "label": "PIX Envio",
        "params": [
            {
                "id": "hfim",
                "label": "Hora fim transação.",
                "type": "date"
            },
            {
                "id": "hinic",
                "label": "Hora início transação.",
                "type": "date"
            }
        ]
    },
    {
        "id": "pix_recebido",
        "label": "PIX Recebido",
        "params": [
            {
                "id": "hfim",
                "label": "Hora fim transação.",
                "type": "date"
            },
            {
                "id": "hinic",
                "label": "Hora início transação.",
                "type": "date"
            }
        ]
    },
    {
        "id": "tipo_pessoa",
        "label": "Tipo de pessoa",
        "params": [
            {
                "id": "ctpo_pssoa",
                "label": "Código do tipo de pessoa (PF ou PJ)",
                "type": "text"
            }
        ]
    }
]

// const ruleDefinitions = [
//     {
//         value: "idade",
//         label: "Idade",
//         type: "number"
//     },
//     {
//         value: "renda",
//         label: "Renda",
//         type: "number"
//     },
//     {
//         value: "cidade",
//         label: "Cidade",
//         type: "text"
//     },
//     {
//         value: "produto",
//         label: "Produto",
//         type: "text"
//     },
//     {
//         value: "data_cadastro",
//         label: "Data de cadastro",
//         type: "date"
//     }
// ];

// const operators = {
//     number: [
//         "igual a",
//         "diferente de",
//         "maior que",
//         "maior ou igual a",
//         "menor que",
//         "menor ou igual a"
//     ],
//     text: [
//         "igual a",
//         "diferente de",
//         "contém",
//         "não contém"
//     ],
//     date: [
//         "igual a",
//         "antes de",
//         "depois de",
//         "antes ou igual a",
//         "depois ou igual a"
//     ]
// };


/* =========================================================
   CREATE GROUP
   ========================================================= */

function createGroup(isRoot = false) {

    const id = ++groupId;

    const group = document.createElement("div");

    group.className = `group ${isRoot ? "root" : ""}`;
    group.dataset.type = "group";
    group.dataset.id = id;

    group.innerHTML = `
        <div class="group-header">

            <select class="form-select form-select-sm logic">
                <option value="AND">AND</option>
                <option value="OR">OR</option>
            </select>
            
            ${isRoot ? `<span class="group-title">Root group</span>` : `<span class="group-title">Group</span>`}

            <div class="actions ms-auto">
                ${
                    !isRoot
                        ? `
                            <button
                                class="btn btn-sm btn-outline-danger"
                                onclick="removeElement(this)"
                            >
                                Delete
                            </button>
                        `
                        : ""
                }
            </div>

        </div>

        <div class="group-children"></div>

        <div class="add-actions px-3">
                ${
                    !isRoot
                        ? `
                            <button
                                class="btn btn-sm btn-outline-primary btn-add"
                                onclick="addCondition(this)"
                            >
                                + Add condition
                            </button>
                        `
                        : ""
                }
            <button
                class="btn btn-sm btn-outline-secondary btn-add"
                onclick="addGroup(this)"
            >
                + Add group
            </button>
        </div>
    `;

    return group;
}


/* =========================================================
   CREATE CONDITION
   ========================================================= */

function createCondition() {

    const id = ++conditionId;

    const row = document.createElement("div");

    row.className = "rule-row";
    row.dataset.type = "condition";
    row.dataset.id = id;

    row.innerHTML = `
        <div class="rule-content">

            <select
                class="form-select form-select-sm rule-select"
                onchange="updateOperator(this)"
            >
                <option value="">Select rule...</option>

                ${rules.map(rule => `
                    <option
                        value="${rule.id}"
                    >
                        ${rule.label}
                    </option>
                `).join("")}

            </select>
            <div class="rule-parameters"></div>

        </div>

        <button
            class="btn btn-sm btn-outline-danger"
            onclick="removeElement(this)"
        >
            Delete
        </button>
    `;

    return row;
}


/* =========================================================
   ADD CONDITION
   ========================================================= */

function addCondition(button) {

    const group = button.closest(".group");
    const container = group.querySelector(":scope > .group-children");

    const condition = createCondition();

    container.appendChild(condition);

}


/* =========================================================
   ADD GROUP
   ========================================================= */

function addGroup(button) {

    const parentGroup = button.closest(".group");
    const container = parentGroup.querySelector(":scope > .group-children");

    const group = createGroup(false);

    container.appendChild(group);

}


/* =========================================================
   REMOVE
   ========================================================= */

function removeElement(button) {

    const element = button.closest(
        '[data-type="condition"], [data-type="group"]'
    );

    if (!element) {
        return;
    }

    element.remove();
}


/* =========================================================
   UPDATE OPERATOR
   ========================================================= */

function getInputType(type) {

    switch (type) {
        case "date":
            return "datetime-local";

        case "number":
            return "number";

        default:
            return "text";
    }
}

function updateOperator(ruleSelect) {

    const row = ruleSelect.closest(".rule-row");
    const parametersContainer = row.querySelector(".rule-parameters");

    const ruleId = ruleSelect.value;

    parametersContainer.innerHTML = "";

    if (!ruleId) {
        return;
    }

    const rule = rules.find(rule => rule.id === ruleId);

    if (!rule) {
        return;
    }

    rule.params.forEach(param => {

        const labelInput = document.createElement("input");

        labelInput.type = "text";
        labelInput.className = "form-control form-control-sm parameter-label";
        labelInput.value = param.label;
        labelInput.readOnly = true;

        const valueInput = document.createElement("input");

        valueInput.type = getInputType(param.type);
        valueInput.className = "form-control form-control-sm parameter-value";
        valueInput.dataset.paramId = param.id;
        valueInput.placeholder = "Value";

        parametersContainer.appendChild(labelInput);
        parametersContainer.appendChild(valueInput);
    });
}


/* =========================================================
   SERIALIZE TREE
   ========================================================= */
function serializeGroup(groupElement) {

    const operator = groupElement.querySelector(
        ":scope > .group-header .logic"
    ).value;

    const children = [];

    const container = groupElement.querySelector(
        ":scope > .group-children"
    );

    Array.from(container.children).forEach(element => {

        /* =========================
           CONDITION
           ========================= */

        if (element.dataset.type === "condition") {

            const ruleSelect = element.querySelector(".rule-select");

            const ruleId = ruleSelect.value;

            if (!ruleId) {
                return;
            }

            const params = {};

            const parameterValues = element.querySelectorAll(
                ".parameter-value"
            );

            parameterValues.forEach(input => {
                params[input.dataset.paramId] = input.value;
            });

            children.push({
                type: "condition",
                rule: ruleId,
                params: params
            });
        }


        /* =========================
           GROUP
           ========================= */

        if (element.dataset.type === "group") {

            children.push(
                serializeGroup(element)
            );
        }

    });

    return {
        type: "group",
        operator: operator,
        children: children
    };
}


/* =========================================================
   INITIALIZE
   ========================================================= */

const root = createGroup(true);

document
    .getElementById("ruleBuilder")
    .appendChild(root);


/* =========================================================
   EXAMPLE
   ========================================================= */

// adiciona duas condições inicialmente
// addCondition(
//     root.querySelector(".btn-add")
// );

// addCondition(
//     root.querySelector(".btn-add")
// );

</script>

</body>
</html>



JSON.stringify(serializeGroup(root))
